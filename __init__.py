'''
Requirements: pyndf, contextlib

'''


from starlink import hds
from contextlib import contextmanager

@contextmanager
def hds_open(fname):
    '''
    Handles automatically releasing memory when the file is closed.

    Actually there is no explicit close method, so this does nothing but it
    fits in nicer with proper pythonic code.
    '''
    yield HDS(fname)

class HDS(object):
    '''
    Encapsulates a read only HDS file.

    This uses the pynds library to analyse an hds file for the information
    contained within. The methods can return data only, not append or update
    the data.
    '''
    def __init__(self, fname, openmode='rb'):
        '''
        Takes a filename parameter and opens the file
        '''
        self.fname = fname
        self.fptr = hds.open(self.fname, openmode)
        self.loc = self.fptr

    def _go_to_name(self, name):
        '''
        Moves the index locator to the requested data point
        '''

        self.loc = self.fptr.find(name)
        assert self.loc.valid

    def _go_to_index(self, index):
        '''
        Moves the index locator to the requested data index
        '''
        assert index < self.fptr.ncomp
        self.loc = self.fptr.index(index)
        assert self.loc.valid

    def __call__(self, name):
        '''
        Alternative to using `self.get`
        '''
        return self.get(name)


    def get(self, name, withtype=False):
        '''
        Retrieves the data stored at that location
        '''
        self._go_to_name(name)
        value = self.loc.get()
        data_type = self.loc.type


        # If the variable is a string, then strip the whitespace
        if 'CHAR' in data_type:
            value = value.strip()

        if withtype:
            return value, data_type
        else:
            return value

    def get_multi(self, names):
        '''
        Returns values for multiple keys
        '''
        results = [self.get(key) for key in names]
        return results

    def structure(self):
        '''
        Prints the file structure
        '''
        n_components = self.fptr.ncomp
        components = []
        for i in xrange(n_components):
            self._go_to_index(i)
            name = self.loc.name
            shape = self.loc.shape

            if shape:
                description = ' '.join([name, str(shape)])
            else:
                description = name
            components.append(description)

        return '\n'.join(components)
