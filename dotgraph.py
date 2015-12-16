import subprocess

class Graphic:
    def __init__(self):
        self.links = dict()

    def link(self, from_entity, to_entity, reverse=False):
        if reverse:
            print "LINK: REVERSING", from_entity, to_entity
            self.link(to_entity, from_entity, reverse=False)
        else:
            print "LINK: ", from_entity, to_entity
            if from_entity not in self.links:
                self.links[from_entity] = set()
            self.links[from_entity].add(to_entity) 

    def draw(self, dotfile):
        dot = open(dotfile, "w")
        dot.write("digraph d {\n")
        allnodes = set()
        for fe in self.links:
            allnodes.add(fe)
            for te in self.links[fe]:
                allnodes.add(te)
        for n in allnodes:
            dot.write('\t"' + n + '";\n')
        for fe in self.links:
            for te in self.links[fe]:
                dot.write('\t"' + fe + '" -> "' + te + '";\n')
        dot.write("}\n")
        dot.close()


if __name__ == "__main__":
    g = Graphic()
    g.link("to2", "to3")
    g.link("to2", "from1", reverse=True)
    g.draw("outfile.dot")
