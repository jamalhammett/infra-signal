from engines.account_engine import AccountEngine,Account
e=AccountEngine()
e.add_account(Account("A001","Allen Hammett Inc."))
assert e.summary()["total_accounts"]==1
print("PASS")
