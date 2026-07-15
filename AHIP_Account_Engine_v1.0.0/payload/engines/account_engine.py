from dataclasses import dataclass,field
from datetime import datetime

@dataclass
class Account:
    account_id:str
    name:str
    industry:str=""
    owner:str=""
    health_score:float=100.0
    created_at:datetime=field(default_factory=datetime.utcnow)

class AccountEngine:
    VERSION="1.0.0"
    def __init__(self):
        self.accounts={}
    def add_account(self,a):
        self.accounts[a.account_id]=a
        return a
    def get_account(self,i):
        return self.accounts.get(i)
    def summary(self):
        return {"total_accounts":len(self.accounts)}
