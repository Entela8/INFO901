from dataclasses import dataclass

@dataclass
class UserEvent:
    sender: int
    lamport: int
    payload: object

@dataclass
class TokenEvent:
    holder: int  # id qui détient le token
