from __future__ import annotations

import contextlib
import runpy
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
from PIL import Image

ROOT = Path(__file__).resolve().parents[1]
MODULE = runpy.run_path(str(ROOT / "scripts/verify_bioclip_pipeline.py"))


class VerifyBioClipPipelineTests(unittest.TestCase):
    def test_model_binding_is_immutable(self) -> None:
        self.assertEqual(MODULE["MODEL_REPO"], "imageomics/bioclip-2.5-vith14")
        self.assertEqual(
            MODULE["MODEL_REVISION"], "191d741545e4c741cdef4b22c6eb69c945c1e592"
        )
        self.assertEqual(sum(item.bytes for item in MODULE["MODEL_FILES"]), 3_944_518_364)

    def test_cli_defaults_to_cpu(self) -> None:
        args = MODULE["build_parser"]().parse_args(
            ["run", "--core-dir", "core", "--d0-dir", "d0"]
        )
        self.assertEqual(args.device, "cpu")

    def test_frozen_environment_keeps_cuda_build_suffixes(self) -> None:
        required = MODULE["FROZEN_DEPENDENCIES"]
        self.assertEqual(required["torch"], "2.11.0+cu128")
        self.assertEqual(required["torchvision"], "0.26.0+cu128")

    def test_safe_payload_path_rejects_escape(self) -> None:
        error_type = MODULE["ValidationError"]
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            accepted = MODULE["safe_payload_path"](root, "payload/image.jpg")
            self.assertEqual(accepted, (root / "payload/image.jpg").resolve())
            with self.assertRaises(error_type):
                MODULE["safe_payload_path"](root, "../image.jpg")

    def test_identical_embedding_passes(self) -> None:
        vector = np.array([1.0, 0.0], dtype=np.float32)
        centroids = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float64)
        species = [
            {"prototype_row": "0", "canonical_taxon_key": "t:0", "scientific_name": "Alpha beta"},
            {"prototype_row": "1", "canonical_taxon_key": "t:1", "scientific_name": "Gamma delta"},
        ]
        result = MODULE["compare_embeddings"](
            np=np,
            current=vector.copy(),
            frozen=vector,
            centroids=centroids,
            species=species,
            min_cosine=0.9999,
            max_abs=0.005,
            max_norm_error=0.002,
        )
        self.assertEqual(result["status"], "PASS")
        self.assertTrue(all(result["checks"].values()))

    def test_embedding_and_top1_drift_fails(self) -> None:
        frozen = np.array([1.0, 0.0], dtype=np.float32)
        current = np.array([0.0, 1.0], dtype=np.float32)
        centroids = np.array([[1.0, 0.0], [0.0, 1.0]], dtype=np.float64)
        species = [
            {"prototype_row": "0", "canonical_taxon_key": "t:0", "scientific_name": "Alpha beta"},
            {"prototype_row": "1", "canonical_taxon_key": "t:1", "scientific_name": "Gamma delta"},
        ]
        result = MODULE["compare_embeddings"](
            np=np,
            current=current,
            frozen=frozen,
            centroids=centroids,
            species=species,
            min_cosine=0.9999,
            max_abs=0.005,
            max_norm_error=0.002,
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertFalse(result["checks"]["cosine"])
        self.assertFalse(result["checks"]["max_abs"])
        self.assertFalse(result["checks"]["top1"])

    def test_absent_model_plan_never_exposes_local_path(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            report = MODULE["model_report"](Path(temporary) / "model")
            rendered = MODULE["json"].dumps(report, sort_keys=True)
        self.assertFalse(report["ready"])
        self.assertEqual(report["download_required_bytes"], 3_944_518_364)
        self.assertNotIn(temporary, rendered)

    def test_pipeline_wiring_on_cpu_without_accelerator_probe(self) -> None:
        class FakeTensor:
            def __init__(self, array: np.ndarray):
                self.array = np.asarray(array)

            def unsqueeze(self, axis: int) -> FakeTensor:
                return FakeTensor(np.expand_dims(self.array, axis))

            def to(self, *args: object, **kwargs: object) -> FakeTensor:
                dtype = kwargs.get("dtype")
                if dtype is None and args:
                    dtype = args[0]
                return FakeTensor(self.array.astype(dtype)) if dtype is not None else self

            def float(self) -> FakeTensor:
                return FakeTensor(self.array.astype(np.float32))

            def detach(self) -> FakeTensor:
                return self

            def cpu(self) -> FakeTensor:
                return self

            def numpy(self) -> np.ndarray:
                return self.array

        class FakeModel:
            visual = types.SimpleNamespace(
                conv1=types.SimpleNamespace(weight=types.SimpleNamespace(dtype=np.float32))
            )

            def eval(self) -> None:
                return None

            def encode_image(self, batch: FakeTensor) -> FakeTensor:
                return FakeTensor(batch.array)

        def normalize(tensor: FakeTensor, dim: int) -> FakeTensor:
            norm = np.linalg.norm(tensor.array, axis=dim, keepdims=True)
            return FakeTensor(tensor.array / norm)

        def inference_mode() -> object:
            return contextlib.nullcontext()

        fake_torch = types.SimpleNamespace(
            cuda=types.SimpleNamespace(
                is_available=lambda: (_ for _ in ()).throw(AssertionError("unexpected CUDA probe"))
            ),
            float16=np.float16,
            inference_mode=inference_mode,
            nn=types.SimpleNamespace(functional=types.SimpleNamespace(normalize=normalize)),
        )
        observed_model_names: list[str] = []

        def create_model_and_transforms(model_name: str, **kwargs: object) -> tuple[object, None, object]:
            observed_model_names.append(model_name)
            self.assertEqual(kwargs["device"], "cpu")
            self.assertEqual(kwargs["precision"], "fp32")
            preprocess = lambda image: FakeTensor(np.array([1.0, 0.0], dtype=np.float32))
            return FakeModel(), None, preprocess

        fake_open_clip = types.SimpleNamespace(
            create_model_and_transforms=create_model_and_transforms
        )
        run_pipeline = MODULE["run_pipeline"]
        globals_dict = run_pipeline.__globals__
        original = tuple(
            globals_dict[name]
            for name in (
                "EXPECTED_QUERY_ROWS",
                "EXPECTED_PROTOTYPE_ROWS",
                "EXPECTED_EMBEDDING_DIM",
            )
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "index").mkdir()
            np.save(root / "index/d0-query-features-fp16.npy", np.array([[1, 0]], dtype=np.float16))
            np.save(
                root / "index/final-species-centroids-f64.npy",
                np.array([[1, 0], [0, 1]], dtype=np.float64),
            )
            (root / "index/species-prototype-map.tsv").write_text(
                "prototype_row\tcanonical_taxon_key\tscientific_name\n"
                "0\tt:0\tAlpha beta\n1\tt:1\tGamma delta\n",
                encoding="utf-8",
            )
            image_path = root / "image.png"
            Image.new("RGB", (2, 2), color="white").save(image_path)
            try:
                globals_dict["EXPECTED_QUERY_ROWS"] = 1
                globals_dict["EXPECTED_PROTOTYPE_ROWS"] = 2
                globals_dict["EXPECTED_EMBEDDING_DIM"] = 2
                with mock.patch.dict(
                    sys.modules, {"open_clip": fake_open_clip, "torch": fake_torch}
                ):
                    result = run_pipeline(
                        core_dir=root,
                        model_dir=root / "model",
                        audit={"tensor_row": 0, "image": image_path},
                        requested_device="cpu",
                        min_cosine=0.9999,
                        max_abs=0.005,
                        max_norm_error=0.002,
                    )
            finally:
                for name, value in zip(
                    (
                        "EXPECTED_QUERY_ROWS",
                        "EXPECTED_PROTOTYPE_ROWS",
                        "EXPECTED_EMBEDDING_DIM",
                    ),
                    original,
                ):
                    globals_dict[name] = value
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["device_category"], "cpu")
        self.assertEqual(len(observed_model_names), 1)
        self.assertTrue(observed_model_names[0].startswith("local-dir:"))


if __name__ == "__main__":
    unittest.main()
