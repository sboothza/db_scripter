# Database Scripter and Diff

## Functionality

- Import Db from source
- Write schema to file
- Compare source to schema file
- Write scripts out to file, based on dependencies, and supply version info
- Write scripts out to file for source control
- Read scripts from files and import into schema file
- Execute scripts from disk in order, and bump version


## Building
`python -m build `

## Deploying
`python -m twine upload --repository pypi dist/*`