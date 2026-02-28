from types import SimpleNamespace
from easycopy.execution.orchestrator import execute_copy
src = SimpleNamespace(records=[{"id":1,"val":10}], path="source")
tgt = SimpleNamespace(kind="FEATURE_SERVICE", manager=SimpleNamespace(truncate=lambda: None), edit_features=lambda **kw: {"success": True}, records=[{"id":1,"val":9}], indexed_fields=[])
payload = {"copy_method":"TRUNCATE_APPEND","source":src,"target":tgt,"batch_size":200}
print(execute_copy(payload, logger=__import__("logging").getLogger("ec")))