"""Local checks of source contracts, batch expectations and replay semantics (no Spark dependency)."""
import json
import sys
import unittest
from datetime import datetime,timedelta,timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'notebooks'))
from ecomsphere.contracts import SOURCES
from ecomsphere.fixtures import SNAPSHOTS, EVENTS, UPPER, EXPECTED
from ecomsphere.context import Context
from ecomsphere.operations import reset


def parse(ts):
    return datetime.fromisoformat(ts.replace('Z','+00:00')).replace(tzinfo=timezone.utc)


def valid_total(row):
    # Decimal comparison includes the one-cent boundary from DQ rule 36.
    gross, discount, tax, shipping, total = (Decimal(row[x]) for x in ['total_item_amount','total_discount_amount','total_tax_amount','total_shipping_amount','grand_total_amount'])
    return abs(total-(gross-discount+tax+shipping)) <= Decimal('0.01')


class FixtureTests(unittest.TestCase):
    def test_schema_catalog(self):
        contracts = json.loads((ROOT/'config/schema_contracts.json').read_text())
        self.assertEqual(len(contracts),56)
        self.assertEqual(sum(len(x['columns']) for x in contracts),910)
        self.assertEqual(len(json.loads((ROOT/'config/dq_rule_catalog.json').read_text())),50)
        # Exact relational source names match the supplied Bronze business column list.
        by_name = {x['name']:x['columns'] for x in contracts}
        for dataset in SOURCES:
            cols = set(SNAPSHOTS[1][dataset][0])
            self.assertEqual(cols,set(by_name['bronze_'+dataset][:-10]))

    def test_three_business_outcomes(self):
        # Independently derive the expected sales and rejects from each source snapshot.
        for b,snapshot in SNAPSHOTS.items():
            customers = {r['customer_id'] for r in snapshot['customer'] if r['is_deleted']=='0'}
            variants = {r['product_variant_id'] for r in snapshot['product_variant'] if r['is_active']=='1'}
            valid_orders = {r['order_id'] for r in snapshot['orders'] if valid_total(r) and r['customer_id'] in customers}
            lines = [r for r in snapshot['order_item'] if r['order_id'] in valid_orders and r['product_variant_id'] in variants]
            rejects = len(snapshot['orders'])-len(valid_orders)+len(snapshot['order_item'])-len(lines)
            self.assertEqual((len(lines),sum(Decimal(r['line_total_amount']) for r in lines),rejects),EXPECTED[b][:3])
            self.assertEqual(len({EVENTS[i]['event_id'] for i in range(1,b+1)}),EXPECTED[b][3])
            self.assertEqual(len({(r['customer_id'],r['email'].strip().lower()) for i in range(1,b+1) for r in SNAPSHOTS[i]['customer']}),EXPECTED[b][4])

    def test_overlap_and_logical_batch_replay(self):
        # Model append identity as (dataset, logical batch, raw row); verify overlap and no duplicate replay.
        identities=set(); prior=parse('1900-01-01 00:00:00'); counts=[]
        for batch in (1,2,3,3):
            upper=parse(UPPER[batch]); lower=prior-timedelta(minutes=30)
            for name,rows in SNAPSHOTS[batch].items():
                for row in rows:
                    if lower<=parse(row['updated_at'])<=upper:
                        identities.add((name,batch,json.dumps(row,sort_keys=True)))
            counts.append(tuple(sum(x[0]==name for x in identities) for name in ('customer','orders','order_item')))
            prior=upper
        self.assertEqual(counts,[(2,2,2),(4,6,6),(5,8,8),(5,8,8)])

    def test_tolerance_and_recovery(self):
        self.assertTrue(valid_total(SNAPSHOTS[1]['orders'][1])) # 110.01 against 110.00
        self.assertFalse(valid_total(SNAPSHOTS[2]['orders'][3])) # 999.00
        self.assertTrue(valid_total(SNAPSHOTS[3]['orders'][3])) # 110.00

    def test_payment_shipment_grain(self):
        for data in SNAPSHOTS.values():
            self.assertEqual(len([r for r in data['payment'] if r['order_id']=='O001']),2)
            self.assertEqual(len([r for r in data['shipment'] if r['order_id']=='O001']),2)
            self.assertEqual(len([r for r in data['order_item'] if r['order_id']=='O001']),1)

    def test_namespace_and_reset_guard(self):
        with self.assertRaises(ValueError):
            Context(None,None,'workspace','production','123',1)
        with self.assertRaises(ValueError):
            Context(None,None,'workspace','ecomsphere17_dev','123',4)
        class Fake:
            prefix='ecomsphere17_dev'
        with self.assertRaises(ValueError):
            reset(Fake(),'NO') # must reject before any SQL


if __name__=='__main__':
    unittest.main()
