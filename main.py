from pydrive.drive import GoogleDrive
from pydrive.auth import GoogleAuth
import numpy as np
import sys
import threading


class Produtor(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, idp, n_files, size_file, drive, contador, fila, fila_locker):
		threading.Thread.__init__(self)
		self.idp = idp
		self.n_files = n_files
		self.size_file = size_file
		self.drive = drive
		self.contador = contador
		self.fila = fila
		self.fila_locker = fila_locker
		print 'Criado: Produtor ' + str(self.idp)
		# self.http = self.drive.auth.Get_Http_Object()

	def run(self):
		for x in range(self.n_files):
			file_name = 'p' + str(self.idp) + 'f' + str(x) + '.txt'
			file = self.drive.CreateFile({'title': file_name})  # Create GoogleDriveFile instance with title 'Hello.txt'.
			content = ''
			for y in range(self.size_file):
				content += str(np.random.randint(0, 1000)) + " "
				# content += str(np.random.randint(0, sys.maxint)) + " "

			file.SetContentString(content) # Set content of the file from given string.
			file.Upload()
			print 'Criado arquivo: ' + file_name
			# print 'Produtor ' + str(self.idp) + ' requisitando lista'
			self.fila_locker.acquire()
			# print 'Produtor ' + str(self.idp) + ' adquiriu lista'
			self.fila.SetContentString(self.fila.GetContentString() + file_name + ' ')
			self.fila.Upload()
			# print 'Produtor ' + str(self.idp) + ' liberando lista'
			self.fila_locker.release()
			# print 'Produtor ' + str(self.idp) + ' liberou lista'
		print 'Morto: Produtor ' + str(self.idp)

class Consumidor(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, idp, drive, contador, fila, fila_locker, contador_locker):
		threading.Thread.__init__(self)
		self.idp = idp
		self.drive = drive
		self.contador = contador
		self.fila = fila
		self.fila_locker = fila_locker
		self.contador_locker = contador_locker
		self.working_on = ''

		print 'Criado: Consumidor ' + str(self.idp)

	def run(self):

		while(True):
			# print 'Consumidor ' + str(self.idp) + ' requisitando contador'
			self.contador_locker.acquire()
			# print 'Consumidor ' + str(self.idp) + ' adquiriu contador'

			restantes = int(self.contador.GetContentString())

			if restantes > 0:
				# print 'Consumidor ' + str(self.idp) + ' requisitando lista'
				self.fila_locker.acquire()
				# print 'Consumidor ' + str(self.idp) + ' adquiriu lista'
				arquivos = self.fila.GetContentString().split(' ')[:-1]
				# print arquivos 
				if len(arquivos) > 1:
					self.working_on = arquivos[1]
					print "Consumidor " + str(self.idp) + " Working on: " + self.working_on

					content = ' '

					for i in arquivos[2:]:
			 			content += i + ' '

			 		# print "Nova fila: " + content

			 		self.fila.SetContentString(content)
			 		self.fila.Upload()
					# print 'Consumidor ' + str(self.idp) + ' liberando lista'
					self.fila_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou lista'

					print 'Arquivo ' + self.working_on + ' consumido por Consumidor: ' + str(self.idp)

					self.contador.SetContentString(str(restantes - 1))
					self.contador.Upload()
				else:
					# print 'Consumidor ' + str(self.idp) + ' liberando lista'
					self.fila_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou lista'
					# print 'Consumidor ' + str(self.idp) + ' liberando contador'
					self.contador_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou contador'
					continue

			else:
				# print 'Consumidor ' + str(self.idp) + ' liberando contador'
				self.contador_locker.release()
				# print 'Consumidor ' + str(self.idp) + ' liberou contador'
				print 'Morto: Consumidor ' + str(self.idp)
				break
			
			# print 'Consumidor ' + str(self.idp) + ' liberando contador'
			self.contador_locker.release()
			# print 'Consumidor ' + str(self.idp) + ' liberou contador'

			files = self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

			for file in files:
				if file['title'] == self.working_on:

					numbers = file.GetContentString().split(' ')[:-1]

			 		file_content = [ int(n) for n in numbers]

			 		sorted_file = np.sort(file_content)

			 		content = ''

			 		for i in sorted_file:
			 			content += str(i) + ' '

			 		file.SetContentString(content)

					file['title'] = file['title'] + "_"
					file.Upload()


class Concatenador(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, drive):
		threading.Thread.__init__(self)
		self.drive = drive

	def run(self):

		final_file = self.drive.CreateFile({'title': 'Concatenado.txt'})
		final_file.Upload()

		file_list = self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

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

	fila_locker = threading.Lock()
	contador_locker = threading.Lock()

	gauth = GoogleAuth()
	gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
	drive = GoogleDrive(gauth)

	contador = drive.CreateFile({'title': 'Contador.txt'})
	contador.SetContentString(str(n_files*n_produtores))
	contador.Upload()

	fila = drive.CreateFile({'title': 'Fila.txt'})
	fila.SetContentString(' ')
	fila.Upload()

	produtores = []
	consumidores = []

	try:
		for x in range(n_produtores):
			produtores.append(Produtor(x, n_files, file_size, drive, contador, fila, fila_locker))
			produtores[x].start()

		for x in xrange(n_consumidores):
			consumidores.append(Consumidor(x, drive, contador, fila, fila_locker, contador_locker))
			consumidores[x].start()
	except:
		print 'Unable to start Thread' 

	for x in range(n_produtores):
		produtores[x].join()

	for x in range(n_consumidores):
		consumidores[x].join()

	print 'FIM'



if __name__ == '__main__':
	main(sys.argv)


