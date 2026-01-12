# core/utils.py

# base58 编码（用于邀请码）
ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def b58encode(num: int) -> str:
    if num == 0:
        return ALPHABET[0]
    arr = []
    base = len(ALPHABET)
    while num:
        num, rem = divmod(num, base)
        arr.append(ALPHABET[rem])
    arr.reverse()
    return "".join(arr)

def b58decode(s: str) -> int:
    base = len(ALPHABET)
    num = 0
    for ch in s:
        num = num * base + ALPHABET.index(ch)
    return num