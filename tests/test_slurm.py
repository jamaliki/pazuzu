from __future__ import annotations

import unittest

from pazuzu import SlurmJob, SlurmResources
from pazuzu.slurm import parse_sacct, parse_squeue, render_slurm_script


class SlurmTests(unittest.TestCase):
    def resources(self, **changes: object) -> SlurmResources:
        values = {
            "memory_gb_per_node": 32,
            "time_limit": "02:00:00",
            "cpus_per_task": 4,
            "gpus_per_node": 1,
            "partition": "gpu",
        }
        values.update(changes)
        return SlurmResources(**values)  # type: ignore[arg-type]

    def test_render_is_deterministic_and_shell_quotes_values(self) -> None:
        job = SlurmJob(
            name="render-smoke",
            argv=("python3", "train.py", "--label", "hello world"),
            cwd="/remote/repo with spaces",
            log_dir="/remote/logs",
            resources=self.resources(),
            environment={"Z_LAST": "two words", "A_FIRST": "one"},
        )

        script = render_slurm_script(job)

        self.assertIn("#SBATCH --cpus-per-task=4", script)
        self.assertIn("#SBATCH --mem=32G", script)
        self.assertIn("cd '/remote/repo with spaces'", script)
        self.assertLess(script.index("export A_FIRST"), script.index("export Z_LAST"))
        self.assertIn("exec python3 train.py --label 'hello world'", script)

    def test_resources_and_directives_reject_unsafe_values(self) -> None:
        with self.assertRaisesRegex(ValueError, "positive integer"):
            self.resources(memory_gb_per_node=0)
        with self.assertRaisesRegex(ValueError, "without whitespace"):
            self.resources(partition="gpu\n#SBATCH --exclusive")
        with self.assertRaisesRegex(ValueError, "only ASCII"):
            SlurmJob(
                name="unsafe name",
                argv=("true",),
                cwd="/remote",
                log_dir="/remote/logs",
                resources=self.resources(),
            )
        with self.assertRaisesRegex(ValueError, "sbatch directive"):
            SlurmJob(
                name="unsafe-log",
                argv=("true",),
                cwd="/remote",
                log_dir="/remote/logs\n#SBATCH --exclusive",
                resources=self.resources(),
            )

    def test_job_copies_mutable_inputs(self) -> None:
        argv = ["python3", "train.py"]
        environment = {"MODE": "smoke"}
        job = SlurmJob(
            name="immutable",
            argv=argv,
            cwd="/remote/repo",
            log_dir="/remote/logs",
            resources=self.resources(),
            environment=environment,
        )

        argv.append("--changed")
        environment["MODE"] = "changed"

        self.assertEqual(("python3", "train.py"), job.argv)
        self.assertEqual("smoke", job.environment["MODE"])
        with self.assertRaises(TypeError):
            job.environment["MODE"] = "forbidden"  # type: ignore[index]

    def test_status_parsers_prefer_exact_job_and_mark_terminal(self) -> None:
        queued = parse_squeue("123", "123_4|RUNNING|00:01|01:00:00|gpu-1\n")
        completed = parse_sacct("123", "123.batch|FAILED|00:02|1:0\n123|COMPLETED|00:03|0:0\n")

        self.assertEqual("RUNNING", queued.state)
        self.assertFalse(queued.terminal)
        self.assertEqual("COMPLETED", completed.state)
        self.assertTrue(completed.terminal)


if __name__ == "__main__":
    unittest.main()
