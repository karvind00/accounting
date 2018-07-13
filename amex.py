from ofxclient import Institution
import psycopg2
import uuid
from Crypto.Hash import SHA256
from ofxclient.account import Account
from ofxparse import Transaction
import datetime
import yaml
from calendar import monthrange


def _get_accounts(institution):
    if institution:
        return institution.accounts()
    return [None]


def _get_test_transactions():
    transaction = Transaction()
    transaction.payee = 'Test'
    transaction.date = datetime.datetime.now()
    transaction.amount = 10.00
    return [transaction]


def _get_transactions(account):
    if account:
        previous_month = datetime.datetime.now().month - 1
        number_of_days = monthrange(datetime.datetime.now().year, previous_month)
        statement = a.statement(days=number_of_days[1])
        return statement.transactions
    return _get_test_transactions()


def _get_institution(institution_key):
    return Institution(
        id=config[institution_key]['id'],
        org=config[institution_key]['org'],
        url=config[institution_key]['url'],
        username=config[institution_key]['username'],
        password=config[institution_key]['password']
    )


with open("ofx.yaml", 'r') as stream:
    try:
        config = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)

connection = psycopg2.connect(dbname="accounting", user="admin", password="password", host="localhost")
cursor = connection.cursor()
cursor.execute("select category_id, name from category;")
categories = cursor.fetchall()
cursor.execute("select merchant_id, category_id, name from merchant;")
merchants = list(cursor.fetchall())
accounts = _get_accounts(_get_institution('amex'))

for a in accounts:

    # an ofxparse.Statement object
    for transaction in _get_transactions(a):
        payee = transaction.payee
        merchant = [merchant for merchant in merchants if merchant[2] == payee]
        if not merchant:
            merchant_id = uuid.uuid4()
            cursor.execute("insert into merchant (merchant_id, name) values (%s,%s)", (str(merchant_id), payee))
            merchants.append((merchant_id, None, payee))
        else:
            merchant_id = merchant[0][0]
        try:
            cursor.execute("insert into transaction (transaction_id,merchant_id, date, amount) values (%s,%s,%s,%s)",
                           (str(uuid.uuid4()), str(merchant_id), transaction.date, transaction.amount))
            connection.commit()
        except psycopg2.IntegrityError as ie:
            if 'duplicate key value violates unique constraint' in ie.args[0]:
                print("Transaction already exists. Merchant Id:{} Date:{} Amount:{}".format(str(merchant_id), transaction.date, transaction.amount))
                connection.rollback()
                pass
            else:
                connection.rollback()
                raise
cursor.close()
connection.close()
