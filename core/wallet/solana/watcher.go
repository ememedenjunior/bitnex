package solana

import (
	"context"
	"cryptohub/core/ledger"
	"log"
	"sync"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/gagliardetto/solana-go/rpc/ws"
)

type Watcher struct {
	wsClient  *ws.Client
	rpcClient *rpc.Client

	seen sync.Map

	queue   chan *rpc.GetTransactionResult
	workers int

	ledger ledger.Ledger
}

type Event struct {
	Type   string
	Mint   string
	From   string
	To     string
	Amount float64
	Tx     string
}

func NewWatcher(rpcURL, wsURL string, workers int, ledger ledger.Ledger) (*Watcher, error) {
	ctx := context.Background()

	wsClient, err := ws.Connect(ctx, wsURL)
	if err != nil {
		return nil, err
	}

	return &Watcher{
		wsClient:  wsClient,
		rpcClient: rpc.New(rpcURL),
		queue:     make(chan *rpc.GetTransactionResult, 5000),
		workers:   workers,
		ledger:    ledger,
	}, nil
}

func (w *Watcher) Start(ctx context.Context, wallets []solana.PublicKey) error {
	log.Println("🚀 Solana watcher started")

	for i := 0; i < w.workers; i++ {
		go w.worker()
	}

	for _, wallet := range wallets {
		sub, err := w.wsClient.LogsSubscribeMentions(
			wallet,
			rpc.CommitmentFinalized,
		)
		if err != nil {
			return err
		}

		go func(wallet solana.PublicKey) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					msg, err := sub.Recv(ctx)
					if err != nil {
						log.Println("WS error:", err)
						time.Sleep(2 * time.Second)
						continue
					}

					go w.processSignature(msg.Value.Signature)
				}
			}
		}(wallet)
	}

	go w.fallbackPoll(ctx, wallets)

	return nil
}

func (w *Watcher) Close() error {
	if w.wsClient != nil {
		w.wsClient.Close()
	}
	return nil
}

func (w *Watcher) fallbackPoll(ctx context.Context, wallets []solana.PublicKey) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, wallet := range wallets {
				sigs, err := w.rpcClient.GetSignaturesForAddress(ctx, wallet)
				if err != nil {
					log.Println("Failed to get signatures for wallet:", wallet.String(), err)
					continue
				}

				for _, s := range sigs {
					go w.processSignature(s.Signature)
				}
			}
		}
	}
}

func (w *Watcher) processSignature(signature solana.Signature) {
	if _, ok := w.seen.Load(signature.String()); ok {
		return
	}
	w.seen.Store(signature.String(), struct{}{})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxVersion := uint64(0)
	tx, err := w.rpcClient.GetTransaction(
		ctx,
		signature,
		&rpc.GetTransactionOpts{
			Encoding:                       solana.EncodingBase64,
			Commitment:                     rpc.CommitmentFinalized,
			MaxSupportedTransactionVersion: &maxVersion,
		},
	)

	if err != nil || tx == nil || tx.Meta == nil {
		return
	}

	select {
	case w.queue <- tx:
	default:
		log.Println("⚠️ queue full:", signature.String())
	}
}

func (w *Watcher) worker() {
	for tx := range w.queue {
		w.decode(tx)
	}
}

func (w *Watcher) decode(tx *rpc.GetTransactionResult) {
	if tx == nil || tx.Meta == nil {
		return
	}

	// Parse the transaction first
	transaction, err := tx.Transaction.GetTransaction()
	if err != nil {
		return
	}

	// Check inner instructions
	inner := tx.Meta.InnerInstructions
	if len(inner) > 0 {
		for _, block := range inner {
			for _, inst := range block.Instructions {
				// Convert rpc.CompiledInstruction to solana.CompiledInstruction
				solanaInst := solana.CompiledInstruction{
					ProgramIDIndex: inst.ProgramIDIndex,
					Accounts:       inst.Accounts,
					Data:           inst.Data,
				}
				w.handle(solanaInst, transaction, tx)
			}
		}
	}

	// Also check top-level instructions (these are already solana.CompiledInstruction)
	for _, inst := range transaction.Message.Instructions {
		w.handle(inst, transaction, tx)
	}
}

func (w *Watcher) handle(inst solana.CompiledInstruction, transaction *solana.Transaction, tx *rpc.GetTransactionResult) {
	if tx == nil || transaction == nil {
		return
	}

	msg := transaction.Message

	if int(inst.ProgramIDIndex) >= len(msg.AccountKeys) {
		return
	}

	programID := msg.AccountKeys[inst.ProgramIDIndex].String()

	switch programID {
	case "11111111111111111111111111111111":
		w.handleSystem(inst, transaction, tx)
	case "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA":
		w.handleSPL(inst, transaction, tx)
	}
}

func (w *Watcher) handleSystem(inst solana.CompiledInstruction, transaction *solana.Transaction, tx *rpc.GetTransactionResult) {
	if tx.Meta == nil || transaction == nil {
		return
	}

	// Resolve accounts for this instruction
	accounts, err := inst.ResolveInstructionAccounts(&transaction.Message)
	if err != nil || len(accounts) < 2 {
		return
	}

	// For System transfer: account indices are typically [from, to]
	// AccountMeta has a PublicKey field, not String() method
	fromAddr := accounts[0].PublicKey.String()
	toAddr := accounts[1].PublicKey.String()

	// Calculate amount from balance changes - find the account index for the sender
	var amount float64
	for i, preBalance := range tx.Meta.PreBalances {
		if i < len(tx.Meta.PostBalances) && tx.Meta.PostBalances[i] < preBalance {
			diff := preBalance - tx.Meta.PostBalances[i]
			amount = float64(diff) / float64(solana.LAMPORTS_PER_SOL)
			break
		}
	}

	// Get signature
	var signature string
	if len(transaction.Signatures) > 0 {
		signature = transaction.Signatures[0].String()
	}

	if amount > 0 {
		w.emit(Event{
			Type:   "SOL",
			From:   fromAddr,
			To:     toAddr,
			Amount: amount,
			Tx:     signature,
		})
	}
}

func (w *Watcher) handleSPL(inst solana.CompiledInstruction, transaction *solana.Transaction, tx *rpc.GetTransactionResult) {
	if tx.Meta == nil || transaction == nil {
		return
	}

	// Parse token balance changes
	preBalances := tx.Meta.PreTokenBalances
	postBalances := tx.Meta.PostTokenBalances

	if len(preBalances) == 0 && len(postBalances) == 0 {
		return
	}

	// Look for token balance changes
	var fromAddr, toAddr, mint string
	var amount float64

	// Create maps for easy lookup
	preMap := make(map[string]rpc.TokenBalance)
	postMap := make(map[string]rpc.TokenBalance)

	for _, bal := range preBalances {
		if bal.Owner != nil {
			preMap[bal.Owner.String()] = bal
		}
	}
	for _, bal := range postBalances {
		if bal.Owner != nil {
			postMap[bal.Owner.String()] = bal
		}
	}

	// Find transfers by comparing pre and post balances
	for ownerKey, pre := range preMap {
		if post, exists := postMap[ownerKey]; exists {
			// Check if balance decreased (sender)
			if pre.UiTokenAmount.Amount != "" && post.UiTokenAmount.Amount != "" {
				// Parse amounts as strings (they are in base units)
				// For simplicity, use UiAmount if available
				if pre.UiTokenAmount.UiAmount != nil && post.UiTokenAmount.UiAmount != nil {
					if *post.UiTokenAmount.UiAmount < *pre.UiTokenAmount.UiAmount {
						fromAddr = ownerKey
						amount = *pre.UiTokenAmount.UiAmount - *post.UiTokenAmount.UiAmount
						mint = pre.Mint.String() // Convert PublicKey to string
					}
				}
			}
		} else {
			// Balance disappeared (sender, may have sent all)
			fromAddr = ownerKey
			if pre.UiTokenAmount.UiAmount != nil {
				amount = *pre.UiTokenAmount.UiAmount
				mint = pre.Mint.String() // Convert PublicKey to string
			}
		}
	}

	for ownerKey, post := range postMap {
		if _, exists := preMap[ownerKey]; !exists {
			// New balance appeared (receiver)
			toAddr = ownerKey
			if mint == "" {
				mint = post.Mint.String() // Convert PublicKey to string
			}
		}
	}

	// If we couldn't find the receiver from balance changes, try instruction accounts
	if toAddr == "" {
		accounts, err := inst.ResolveInstructionAccounts(&transaction.Message)
		if err == nil && len(accounts) >= 2 {
			// SPL transfer typically: [source, destination]
			// AccountMeta has a PublicKey field
			if fromAddr == accounts[0].PublicKey.String() {
				toAddr = accounts[1].PublicKey.String()
			}
		}
	}

	// Get signature
	var signature string
	if len(transaction.Signatures) > 0 {
		signature = transaction.Signatures[0].String()
	}

	if amount > 0 && fromAddr != "" && toAddr != "" {
		w.emit(Event{
			Type:   "SPL",
			Mint:   mint,
			From:   fromAddr,
			To:     toAddr,
			Amount: amount,
			Tx:     signature,
		})
	}
}

func (w *Watcher) emit(e Event) {
	log.Printf("💰 EVENT: Type=%s, From=%s, To=%s, Amount=%f, Tx=%s",
		e.Type, e.From, e.To, e.Amount, e.Tx)

	if e.To == "" {
		log.Println("⚠️ No destination address found, skipping")
		return
	}

	// Find user by wallet address
	userID, err := w.ledger.FindUserByWallet(context.Background(), e.To)
	if err != nil {
		log.Printf("❌ Error finding user for wallet %s: %v", e.To, err)
		return
	}
	if userID == "" {
		log.Printf("⚠️ No user found for wallet: %s", e.To)
		return
	}

	// Determine asset type
	asset := e.Type
	if e.Mint != "" {
		asset = e.Mint
	}

	// Check if transaction already processed (prevent duplicates)
	existingTx, err := w.ledger.GetTransactionByReference(context.Background(), e.Tx)
	if err != nil {
		log.Printf("⚠️ Error checking existing transaction: %v", err)
	}
	if existingTx != nil {
		log.Printf("⚠️ Transaction %s already processed, skipping", e.Tx)
		return
	}

	// Credit the account using the ledger's CreditAccount method
	err = w.ledger.CreditAccount(
		context.Background(),
		userID,
		asset,
		e.Amount,
		e.Tx,
		"deposit",
	)
	if err != nil {
		log.Printf("❌ Failed to credit account: %v", err)
		return
	}

	log.Printf("✅ Successfully credited %f %s to user %s (wallet: %s, tx: %s)",
		e.Amount, asset, userID, e.To, e.Tx)
}
