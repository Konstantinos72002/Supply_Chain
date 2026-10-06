"""
Local stand-in for the Lakeflow runtime: executes the pipeline source files exactly as written
(pipelines/medallion/*.py) with a minimal `dlt` module, resolving datasets in dependency order
across files - like one pipeline update. Auto Loader is replaced by a batch read with the same
options, and spark.read.table() resolves to the datasets computed so far.
"""
import glob
import os
import sys
import types

PIPELINE_DIR = os.path.join(os.path.dirname(__file__), "..", "pipelines", "medallion")
SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src")


class _AutoLoaderAsBatch:
    def __init__(self, spark):
        self.spark, self.opts = spark, {}

    def format(self, _):
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def load(self, path):
        fmt = self.opts.pop("cloudFiles.format")
        opts = {k: v for k, v in self.opts.items() if not k.startswith("cloudFiles.")}
        return self.spark.read.format(fmt).options(**opts).load(path)


def run_pipeline(spark, conf: dict) -> dict:
    """Returns {"tables": {name: DataFrame}, "expectations": {name: {exp: failed_rows}}, "definitions": [...]}"""
    for k, v in conf.items():
        spark.conf.set(k, v)
    defs = {}

    def table(name, comment=None, table_properties=None, cluster_by=None, temporary=False, **_):
        def deco(fn):
            assert name not in defs, f"dataset {name} defined twice"
            defs[name] = dict(fn=fn, warn=getattr(fn, "_warn", {}), fail=getattr(fn, "_fail", {}),
                              temporary=temporary, cluster_by=cluster_by, comment=comment)
            return fn
        return deco

    def _exp(kind):
        def factory(e):
            def deco(fn):
                setattr(fn, kind, {**getattr(fn, kind, {}), **e})
                return fn
            return deco
        return factory

    dlt = types.ModuleType("dlt")
    dlt.table, dlt.expect_all, dlt.expect_all_or_fail = table, _exp("_warn"), _exp("_fail")
    dlt.view = lambda name, **kw: table(name, temporary=True)
    sys.modules["dlt"] = dlt
    tables = {}

    class _Reader:
        def table(self, name):
            return tables[name]          # KeyError = dependency not computed yet

    class _Spark:
        read = _Reader()

        @property
        def readStream(self):
            return _AutoLoaderAsBatch(spark)

        def __getattr__(self, a):
            return getattr(spark, a)

    files = sorted(glob.glob(os.path.join(PIPELINE_DIR, "*.py")))
    for f in files:
        exec(compile(open(f).read(), f, "exec"), {"spark": _Spark(), "__name__": os.path.basename(f)})

    expectations, pending = {}, list(defs)
    while pending:
        progressed = False
        for name in list(pending):
            d = defs[name]
            try:
                df = d["fn"]()
            except KeyError:
                continue
            df = df.localCheckpoint()
            tables[name] = df
            pending.remove(name)
            progressed = True
            expectations[name] = {k: df.where(f"NOT ({v})").count()
                                  for k, v in {**d["warn"], **d["fail"]}.items()}
            failed = {k: n for k, n in expectations[name].items() if k in d["fail"] and n}
            assert not failed, f"fail-expectation violated in {name}: {failed}"
        assert progressed, f"unresolvable dependencies: {pending}"
    return {"tables": tables, "expectations": expectations, "definitions": defs, "files": files}
