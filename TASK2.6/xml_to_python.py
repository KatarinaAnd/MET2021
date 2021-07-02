import pandas as pd
import numpy as np
import xml.etree.cElementTree as et

tree=et.parse('20201120T051007.xml')
et.register_namespace('', '{http://www.aadi.no/RTOutSchema}')
root=tree.getroot()

#Value_list = []

# SensorData = 
#et.register_namespace("", '{http://www.aadi.no/RTOutSchema}')


#for child in root[3][2][0].iter('Point'):
#    print(child.tag)
#    print(child.tag, child.attrib)

#print(root.iter)
#print(Value_list)
#rint(root)
namespace= {
    'point' : '{http://www.aadi.no/RTOutSchema}'
}
for ns in root.findall('point:Point', namespace):
    name = ns.find('point:Point', namespace)
    print(name.text)