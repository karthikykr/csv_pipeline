import subprocess
import sys


def run_step(name: str, cmd: list[str]) -> None:
    print(f"\nStarting: {name}")
    result = subprocess.run(cmd)

    if result.returncode != 0:
        print(f"Failed: {name}")
        sys.exit(result.returncode)

    print(f"Completed: {name}")


def main():
    run_step(
        "CSV Splitting",
        [sys.executable, "split.py"],
    )

    run_step(
        "Normalization",
        [sys.executable, "clean.py"],
    )

    print("\nPipeline finished successfully")


if __name__ == "__main__":
    main()
