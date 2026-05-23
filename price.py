import requests

response = requests.get(
    "https://api.coingecko.com/api/v3/simple/price",
    params={
        "ids": "solana",
        "vs_currencies": "usd"
    }
)

data = response.json()

print("SOL Price:", data["solana"]["usd"])