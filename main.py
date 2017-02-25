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
	def __init__(self, idp, drive, contador, fila, fila_locker, contador_locker, fila_conc, fila_conc_locker):
		threading.Thread.__init__(self)
		self.idp = idp
		self.drive = drive
		self.contador = contador
		self.fila = fila
		self.fila_locker = fila_locker
		self.contador_locker = contador_locker
		self.fila_conc = fila_conc
		self.fila_conc_locker = fila_conc_locker

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

					file_name = file['title'] + "_"
					file['title'] = file_name 
					file.Upload()

					self.fila_conc_locker.acquire()
					self.fila_conc.SetContentString(self.fila_conc.GetContentString() + file_name + ' ')
					self.fila_conc.Upload()
					self.fila_conc_locker.release()
					


class Concatenador(threading.Thread):
	"""docstring for Produtor"""
	def __init__(self, drive, concatenador, fila_conc, fila_conc_locker, cont_conc, cont_conc_locker):
		threading.Thread.__init__(self)
		self.drive = drive
		self.concatenador = concatenador
		self.fila_conc = fila_conc
		self.fila_conc_locker = fila_conc_locker
		self.cont_conc = cont_conc
		self.cont_conc_locker = cont_conc_locker
		print 'Criado: Concatenador'

	def run(self):

		while(True):
			# print 'Consumidor ' + str(self.idp) + ' requisitando contador'
			self.cont_conc_locker.acquire()
			# print 'Consumidor ' + str(self.idp) + ' adquiriu contador'

			restantes = int(self.cont_conc.GetContentString())

			if restantes > 0:
				# print 'Consumidor ' + str(self.idp) + ' requisitando lista'
				self.fila_conc_locker.acquire()
				# print 'Consumidor ' + str(self.idp) + ' adquiriu lista'
				arquivos = self.fila_conc.GetContentString().split(' ')[:-1]
				# print arquivos 
				if len(arquivos) > 1:
					self.working_on = arquivos[1]
					print "Concatenador working on: " + self.working_on

					content = ' '

					for i in arquivos[2:]:
			 			content += i + ' '

			 		# print "Nova fila: " + content

			 		self.fila_conc.SetContentString(content)
			 		self.fila_conc.Upload()
					# print 'Consumidor ' + str(self.idp) + ' liberando lista'
					self.fila_conc_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou lista'

					print 'Arquivo ' + self.working_on + ' consumido pelo Concatenador'

					self.cont_conc.SetContentString(str(restantes - 1))
					self.cont_conc.Upload()

				else:
					# print 'Consumidor ' + str(self.idp) + ' liberando lista'
					self.fila_conc_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou lista'
					# print 'Consumidor ' + str(self.idp) + ' liberando contador'
					self.cont_conc_locker.release()
					# print 'Consumidor ' + str(self.idp) + ' liberou contador'
					continue

			else:
				# print 'Consumidor ' + str(self.idp) + ' liberando contador'
				self.cont_conc_locker.release()
				# print 'Consumidor ' + str(self.idp) + ' liberou contador'
				print 'Morto: Concatenador'
				break
			
			# print 'Consumidor ' + str(self.idp) + ' liberando contador'
			self.cont_conc_locker.release()
			# print 'Consumidor ' + str(self.idp) + ' liberou contador'

			files = self.drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()

			for file in files:
				if file['title'] == self.working_on:

					numbers = file.GetContentString().split(' ')[:-1]

					file.Delete()

			 		file_content = [ int(n) for n in numbers]

			 		numbers2 = self.concatenador.GetContentString().split(' ')[:-1]

			 		file_concat = [ int(n) for n in numbers2]

			 		sorted_file = np.sort(file_content + file_concat)

			 		content = ''

			 		for i in sorted_file:
			 			content += str(i) + ' '

			 		self.concatenador.SetContentString(content)
					self.concatenador.Upload()


def main(argv):

	if len(argv) < 5:
		print 'Usage: python main.py {Nuero de produtores} {Numero de consumidores} {Numero de arquivos por produtor} {Tamanho de cada arquivo}'
		sys.exit(0)

	n_produtores = int(argv[1])
	n_consumidores = int(argv[2])
	n_files = int(argv[3])
	file_size = int(argv[4])

	fila_locker = threading.Lock()
	contador_locker = threading.Lock()
	fila_conc_locker = threading.Lock()
	cont_conc_locker = threading.Lock()

	gauth = GoogleAuth()
	gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.
	drive = GoogleDrive(gauth)

	conc_file = drive.CreateFile({'title': 'Arquivo Final.txt'})
	conc_file.Upload()

	contador = drive.CreateFile({'title': 'Contador.txt'})
	contador.SetContentString(str(n_files*n_produtores))
	contador.Upload()

	fila = drive.CreateFile({'title': 'Fila.txt'})
	fila.SetContentString(' ')
	fila.Upload()

	cont_conc = drive.CreateFile({'title': 'Contador_Concatenador.txt'})
	cont_conc.SetContentString(str(n_files*n_produtores))
	cont_conc.Upload()

	fila_conc = drive.CreateFile({'title': 'Fila_Concatenador.txt'})
	fila_conc.SetContentString(' ')
	fila_conc.Upload()


	produtores = []
	consumidores = []
	concatenador = None

	try:
		for x in range(n_produtores):
			produtores.append(Produtor(x, n_files, file_size, drive, contador, fila, fila_locker))
			produtores[x].start()

		for x in xrange(n_consumidores):
			consumidores.append(Consumidor(x, drive, contador, fila, fila_locker, contador_locker, fila_conc, fila_conc_locker))
			consumidores[x].start()

		concatenador = Concatenador(drive, conc_file, fila_conc, fila_conc_locker, cont_conc, cont_conc_locker)
		concatenador.start()
	except:
		print 'Unable to start Thread' 

	for x in range(n_produtores):
		produtores[x].join()

	for x in range(n_consumidores):
		consumidores[x].join()

	concatenador.join()

	fila.Delete()
	contador.Delete()
	fila_conc.Delete()
	cont_conc.Delete()

	print 'FIM'



if __name__ == '__main__':
	main(sys.argv)


