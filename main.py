from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import numpy as np
import sys
import threading


class Produtor(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, idp, n_files, size_file, drive, contador, fila):
		threading.Thread.__init__(self)
		self.idp = idp
		self.n_files = n_files
		self.size_file = size_file
		self.drive = drive
		self.contador = contador
		self.fila = fila
		# self.http = self.drive.auth.Get_Http_Object()

	def run(self):
		for x in range(self.n_files):
			file_name = 'p' + str(self.idp) + 'f' + str(x) + '.txt'
			file = self.drive.CreateFile({'title': file_name})  # Create GoogleDriveFile instance with title 'Hello.txt'.
			content = ''
			for y in range(self.size_file):
				content += str(np.random.randint(0, sys.maxint)) + " "

			file.SetContentString(content) # Set content of the file from given string.
			file.Upload()
			self.fila.SetContentString(self.fila.GetContentString() + ' ' + file_name)
			self.fila.Upload()

class Consumidor(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, drive):
		threading.Thread.__init__(self)
		self.drive = drive

	def run(self):
		file_list = self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
		for file1 in file_list:

			if file1['title'][-1] != '_':
		 		# print('title: %s, id: %s' % (file1['title'], file1.GetContentString()))

		 		numbers = file1.GetContentString().split(' ')[:-1]

		 		file_content = [ int(n) for n in numbers]

		 		sorted_file = np.sort(file_content)

		 		content = ''

		 		for i in sorted_file:
		 			content += str(i) + ' '

		 		print content
		 		file1.SetContentString(content)

				file1['title'] = file1['title'] + "_"
				file1.Upload()

class Concatenador(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, drive):
		threading.Thread.__init__(self)
		self.drive = drive

	def run(self):

		final_file = drive.CreateFile({'title': 'Concatenado.txt'})
		final_file.Upload()

		file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

		for file1 in file_list:

			if file1['title'][-1] == '_':
		 		# print('title: %s, id: %s' % (file1['title'], file1.GetContentString()))

		 		numbers = file1.GetContentString().split(' ')[:-1]
		 		
		 		file_content = [ int(n) for n in numbers]

		 		old = final_file.GetContentString().split(' ')[:-1]

		 		old_int = [ int(n) for n in old]

		 		sorted_file = np.sort(file_content + old_int)

		 		content = ''

		 		for i in sorted_file:
		 			content += str(i) + ' '

		 		file1.Delete()

		 		final_file.SetContentString(content)

				final_file.Upload()



def main(argv):

	n_produtores = int(argv[1])
	n_consumidores = int(argv[2])
	n_files = int(argv[3])
	file_size = int(argv[4])

	gauth = GoogleAuth()
	gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
	drive = GoogleDrive(gauth)

	contador = drive.CreateFile({'title': 'Contador.txt'})
	contador.SetContentString(str(n_files*n_produtores))
	contador.Upload()

	fila = drive.CreateFile({'title': 'Fila.txt'})
	fila.Upload()

	produtores = []

	try:
		for x in range(n_produtores):
			produtores.append(Produtor(x, n_files, file_size, drive, contador, fila))
			produtores[x].start()
	except:
		print 'Unable to start Thread' 


	for x in range(n_produtores):
		produtores[x].join()

	print 'FIM'



if __name__ == '__main__':
	main(sys.argv)


