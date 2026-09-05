from h3i_measurement_contract import APPROVED_PARENT_CONTENT_ROOT
def test_parent():assert APPROVED_PARENT_CONTENT_ROOT=='3eed480628cfa22e7c7d120dd00c4083ab105d67d424bb9ddeffa31c8e871e45'
def test_fx_exempt():
 from h3_history_contracts import note_source_generation
 class O:pass
 r=note_source_generation(O(),market_key='local_fx',symbol='XAUUSD',path='X.DAT',size=68,record_count=2,session_date='2026-08-25');assert r['family_exempt'] and not r['warmup']
