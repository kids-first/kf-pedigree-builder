<p align="center">
  <!-- markdownlint-disable-next-line MD013 -->
  <img src="docs/kids_first_logo.svg" alt="Kids First repository logo" width="660px" />
</p>
<p align="center">
<!-- markdownlint-disable-next-line MD013 -->
  <a href="https://github.com/kids-first/kf-pedigree-builder/blob/main/LICENSE"><img src="https://img.shields.io/github/license/kids-first/kf-pedigree-builder.svg?style=for-the-badge"></a>
</p>

# Kids First Pedigree Builder

:family: Tools (and a python package supporting those tools) to allow querying
Kids First data and building dbGaP-style pedigree files. Allows querying the
open-access Kids First FHIR server, as well as the backend Kids First
Dataservice and PostgreSQL database.

NOTE: Currently, this tool queries data that is only accessible from within the
Kids First DRC. If you would like a pedigree file generated for you, please
contact [our support](mailto:support@kidsfirstdrc.org).

## How to Install

To generate pedigree reports, clone this repo:

```sh
git clone https://github.com/kids-first/kf-utils-python.git
```

## How to Use

n.b. `./pedigree_report build_pedi_report --help` to see detailed usage of
flags and options.

To generate a pedigree file for a study using the Kids First PostgreSQL
database:

```sh
./pedigree_report build_pedi_report \
    --study_id SD_ME0WME0W \
    --db_url $DATABASE_URL \
    --output_file my_pedigree_file.txt
```
