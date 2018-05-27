import string
from random import choice,randint,seed
import numpy as np
from pyblake2 import blake2b
import hashlib

def hash_family(semilla=12345,k=100):
	"""
	Dado una semilla y un número k, 
	genera los salts (semilla en string)
	para generar k funciónes de hash blake2b
	"""
	seed(semilla)
	hash_salts = []
	for i in range(int(k)):
		min_char = 8
		max_char = 12
		allchar = string.ascii_letters + string.digits
		password = "".join(choice(allchar) for x in range(randint(min_char, max_char)))
		salt = password.encode()
		hash_salts.append(salt)

	return hash_salts


def hash_generator(elemento,salts,modulo_primo=526717,hyperloglog=False):
    """
    Recibe un elemento, y genera los hashes blake2b del elemento, se mapean a
    enteros y se les aplica módulo de un primo dado.
    Si hyperloglog es True, regresa un hash binario, uno solo.
    """
    elemento = elemento.encode() 
    
    if hyperloglog == True:
    	temp = hashlib.sha1(elemento.hexdigest())
    	hashes = format(int(temp,16) , '08b')

    else:
    	hashes = [int(blake2b(elemento,salt=salt).hexdigest(),16) % modulo_primo\
	    for salt in salts]
    
    return hashes


class bloom_filter:
	"""
	Un bloom filter.
	
	Recibe los salts generados para loss hashes con blake2b y un primo, que es el largo del]
	vector de bits.
	"""     
	def __init__(self,salts,big_prime):
	    self.salts = salts
	    self.big_prime = big_prime
	    self.bits_vector = np.zeros(self.big_prime)

	def new_observation(self,element):
		"""
		Checa, dado un elemento, 
		si este ya existe, si no, lo inserta.
		"""
		#generate hashes
		hashes = hash_generator(element,self.salts,self.big_prime)
		if self.bits_vector[hashes].sum() == len(hashes):
		    #print('elemento {} ya esta en la lista'.format(element))
		    return 0
		else:
		    #print('elemento {} no esta en la lista'.format(element))
		    #inserción en el filtro de bloom
		    self.bits_vector[hashes] = 1
		    return 1

	def is_in_filter(self,element):
		"""Checa, dado un elemento, 
		si este ya existe, si no, lo inserta."""
		#generate hashes
		hashes = hash_generator(element,self.salts,self.big_prime)
		if self.bits_vector[hashes].sum() == len(hashes):
		#print('elemento {} ya esta en la lista'.format(element))
			return 1
		else:
		#print('elemento {} no esta en la lista'.format(element))
			return 0
    

class hyperloglog:
   
   def __init__(self, lead_bits):
       self.lead_bits = lead_bits
       
   def count(self, data):
       mx = []
       for el in data:
           binary = hash_generator(el, salts=salts, hyperloglog =True)
           lead = binary[1:self.lead_bits]
           tail = binary[self.lead_bits:]
           t = list(tail)
           mx.insert(i,[lead, t.index(max(t))+1])
       #lo hacemos datafrfame.
       mx = pd.DataFrame(mx)
       mx.columns = ['cubeta', 'tailmax']
       #En cada cubeta, sacó el valor maximo de la cola de ceros.
       #De esa, sacó el promedio armonico.
       count = 2**(self.lead_bits-1) * (1/(1/(2**(mx.groupby('cubeta')['tailmax'].max()))).mean()) * 0.72
       return count

class cubeta:
    
    def __init__(self):
        self.values = []
    def add_element(self,record):
        self.values.append(record)
        

def hash_bucket(elemento,num_canastas=10):
    """
    Regresa la cubeta, de acuerdo al hash generado por el elemento.
    """
    elemento = elemento.encode() 
    
    hashes = int(hashlib.sha256(elemento).hexdigest(),16) % num_canastas
    
    return hashes
