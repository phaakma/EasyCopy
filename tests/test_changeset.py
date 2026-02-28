from easycopy.change_detection.changesets import write_changesets
from easycopy.models import ChangeSet
cs = ChangeSet(adds=[{'id':1,'a':2}], updates=[{'id':2,'a':3}], deletes=[{'id':3}])
print(write_changesets(cs, 'tmp_changesets'))