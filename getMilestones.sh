python scan.py 2015-07-21 | python filter.py type ReleaseEvent | python paths.py payload.release.tag_name repo.name created_at | head -3
python scan.py 2013-07-21 | python filter.py type ReleaseEvent | python paths.py payload.release.tag_name repository.owner repository.name created_at | head -3
