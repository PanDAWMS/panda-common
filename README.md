# Panda Common Project

Includes all libraries used by PanDA server, JEDI, Harvester, monitor and others.

## Development

    pip install pre-commit
    pre-commit install

`pre-commit install` wires the hooks into `git commit`. To check the whole tree at any
time, run `pre-commit run --all-files` -- that is exactly what the Syntax Check job runs,
so it tells you whether a PR will be green before you open one.

Do not run the formatters or checkers directly from your own environment. The versions
are pinned in `.pre-commit-config.yaml` and pre-commit installs them into virtualenvs of
its own; an unpinned local tool reports different findings than CI, in both directions.
