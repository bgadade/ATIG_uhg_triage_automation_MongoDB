""" This file contains all the classes necessary for the framework """


class fwrapper:
    """This class serves as the base class for creating the IF construct"""

    def __init__(self, function, childcount, name):
        """Constructor function which initializes the IF construct with the function,
            number of children and the name of the function"""
        self.function = function
        self.childcount = childcount
        self.name = name


class node:
    """node class is the base class which helps in creating the particular "node" of the tree"""

    def __init__(self, fw, children):
        """Constructor of the node class which helps in initializing the node of the tree
        This takes function name and the list of children as the arguments """

        self.function = fw.function  # stores the function body
        self.name = fw.name  # stores the function name
        self.children = children  # gets the children

    def populate(self, inp):
        """this method populates the node with list of the children and the dataframe parameter"""
        results = [n for n in self.children]  # gets the list of children in the reult variable
        return self.function(results, inp)  # calls the function with the results and input dataframe parameters

    def display(self, indent=0):
        """This method displays the execution flo of the tree """
        print((' ' * indent) + self.name)
        for c in self.children:
            c.display(indent + 1)


class paramnode:
    """This class helps to create parameter node i:e the leaf nodes which donot have further children"""

    def __init__(self, idx):
        """paramnode constructor helps in initializing the paramnode object with the id within the
        argument list"""
        self.idx = idx

    def populate(self, inp):
        """This method populates the paramnode with specific id"""
        return inp[self.idx]

    def display(self, indent=0):
        """Displays the paramnode with defined indent"""
        print('%sp%s' % (' ' * indent, self.idx))


class constnode:
    """Constructor helps in creating the constant node with a defined constatnt value"""

    def __init__(self, c):
        self.c = c

    def populate(self, inp):
        """This method populates the node with a constant"""
        return self.c

    def display(self, indent=0):
        """This method displays the constant node with defined indent"""
        print('%s%s' % (' ' * indent, self.c))