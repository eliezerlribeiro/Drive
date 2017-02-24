from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import numpy as np
import sys


def produtor(id, n_files, size_file, drive):
	
	for x in range(n_files):
		file = drive.CreateFile({'title': str(id) + str(x) + '.txt'})  # Create GoogleDriveFile instance with title 'Hello.txt'.
		content = ''
		for y in range(size_file):
			content += str(np.random.randint(0, 100)) + " "

		file.SetContentString(content) # Set content of the file from given string.
		file.Upload()


def consumidor(drive):

	file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
	for file1 in file_list:

		if file1['title'][-1] != '_':
	 		# print('title: %s, id: %s' % (file1['title'], file1.GetContentString()))

	 		numbers = file1.GetContentString().split(' ')[:-1]

	 		file_content = [ int(n) for n in numbers]

	 		sorted_file = np.sort(file_content)

			file1['title'] = file1['title'] + "_"
			file1.Upload()

def concatenador(drive):

	final_file = drive.CreateFile({'title': 'Concatenado.txt'})

	file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

	for file1 in file_list:

		if file1['title'][-1] == '_':
	 		# print('title: %s, id: %s' % (file1['title'], file1.GetContentString()))

	 		numbers = file1.GetContentString().split(' ')[:-1]
	 		
	 		file_content = [ int(n) for n in numbers]

	 		sorted_file = np.sort(file_content)

			file1['title'] = file1['title'] + "_"
			file1.Upload()



def main(argv):

	gauth = GoogleAuth()
	gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
	drive = GoogleDrive(gauth)

	produtor(1, 2, 10, drive)
	consumidor(drive)
	print 'FIM'



if __name__ == '__main__':
	main(sys.argv)


