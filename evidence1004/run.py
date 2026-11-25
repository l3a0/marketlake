import sys, hashlib, pathlib

sys.path.insert(0, sys.argv[1])
sys.path.insert(0, str(pathlib.Path(__file__).parent))
import lake

print(lake.__file__, file=sys.stderr)
import helpers as h

out = pathlib.Path(sys.argv[2])
for surface in h.SCHEMAS:
    for shape in [*h.SHAPES, "a retyped column"]:
        root = out / surface / shape.replace(" ", "_") / "lake"
        h._day(root, surface, shape)
        h._seal(root)
        for k, v in h._tree(root).items():
            print(surface, shape, k, hashlib.sha256(v).hexdigest())
