import time
from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
from pydrive.apiattr import ApiAttributeMixin
from pydrive.apiattr  import ApiResource

gauth = GoogleAuth()
gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.


drive = GoogleDrive(gauth)

file1 = drive.CreateFile({'title': 'Hello.txt'})  # Create GoogleDriveFile instance with title 'Hello.txt'.
file1.SetContentString('Hello World!') # Set content of the file from given string.
a=file1.GetContentString()
print a
a=a.split(' ')
print a[0]
file1.SetContentString(file1.GetContentString()+' '+'Hello World2!') # Set content of the file from given string.
file1.Upload()

time.sleep(5.5) 

file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
for file1 in file_list:
  print('title: %s, id: %s' % (file1['title'], file1['id']))	 
  if file1['title']=='Hello.txt':
	 file1.Trash()

