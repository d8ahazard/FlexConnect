import base64
import json
import operator
import urllib
from datetime import datetime
from logging import getLogger
from bottle import request, response
from xml.sax.saxutils import escape, quoteattr
import xmltodict
Log = getLogger()


class Object(object):

    def __init__(self, **kwargs):
        # Add all class attributes that aren't private or functions
        for key in self.__class__.__dict__:
            if key[0] != "_" and self.__class__.__dict__[key].__class__.__name__ != "function":
                self.__dict__[key] = self.__class__.__dict__[key]
        self.__headers = {}

        # Add all supplied argument attributes, unless the attribute is None and would overwrite an existing attribute
        for key in kwargs:
            if not (self.__dict__.has_key(key) and kwargs[key] == None):
                self.__dict__[key] = kwargs[key]

    def Content(self):
        return None

    def Status(self):
        return "200 OK"

    def Headers(self):
        headerString = ""
        for name in self.__headers:
            headerString += "%s: %s\r\n" % (name, self.__headers[name])
        return headerString

    def SetHeader(self, name, value):
        self.__headers[name] = value

    def __repr__(self):
        keys = self.__dict__.keys()
        repr_str = self.__class__.__name__ + "(%s)"
        arg_str = ""
        for i in range(len(keys)):
            key = keys[i]
            obj = self.__dict__[key]
            if key[0] != "_" and key != "tagName":
                if arg_str != "": arg_str += ", "
                if type(obj).__name__ == "function":
                    arg_str += key + "=" + str(obj.__name__)
                else:
                    arg_str += key + "=" + repr(obj)
        return repr_str % arg_str


####################################################################################################

class Container(Object):

    def __init__(self, **kwargs):
        Object.__init__(self, **kwargs)
        self.__items__ = []

    def __iter__(self):
        return iter(self.__items__)

    def __len__(self):
        return len(self.__items__)

    def __getitem__(self, key):
        return self.__items__[key]

    def __setitem__(self, key, value):
        self.__items__[key] = value

    def __delitem__(self, key):
        del self.__items__[key]

    def Append(self, obj):
        return self.__items__.append(obj)

    def Count(self, x):
        return self.__items__.count(x)

    def Index(self, x):
        return self.__items__.index(x)

    def Extend(self, x):
        return self.__items__.extend(x)

    def Insert(self, i, x):
        return self.__items__.insert(i, x)

    def Pop(self, i):
        return self.__items__.pop(i)

    def Remove(self, x):
        return self.__items__.remove(x)

    def Reverse(self):
        self.__items__.reverse()

    def Sort(self, attr):
        self.__items__.sort(key=operator.attrgetter(attr))


####################################################################################################

class XMLObject(Object):
    def __init__(self, **kwargs):
        self.tagName = self.__class__.__name__
        Object.__init__(self, **kwargs)
        self.SetHeader("Content-Type", "application/xml")

    def SetTagName(self, tagName):
        self.tagName = tagName

    def __str__(self):
        if "key" in self.__dict__:
            return self.key
        else:
            return repr(self)


####################################################################################################

class XMLContainer(XMLObject, Container):
    def __init__(self, **kwargs):
        self.tagName = self.__class__.__name__
        Container.__init__(self, **kwargs)
        self.SetHeader("Content-Type", "application/xml")


class FlexContainer:

    def __init__(self, tag="MediaContainer", attributes=None, show_size=True,
                 allowed_attributes=None, allowed_children=None, limit=False):
        encoding = "xml"
        self.container_start = 0
        self.container_size = False
        for key, value in request.headers.items():
            if (key == "Accept") | (key == "X-Plex-Accept"):
                if (value == "application/json") | (value == "json"):
                    encoding = "json"
                    break
            if (key == "Container-Start") | (key == "X-Plex-Container-Start"):
                self.container_start = int(value)
            if (key == "Container-Size") | (key == "X-Plex-Container-Size"):
                if limit:
                    self.container_size = int(value)
        self.encoding = encoding
        self.tag = tag
        self.child_string = ""
        self.child_strings = []
        self.children = []
        self.attributes = attributes
        self.show_size = show_size
        self.allowed_attributes = allowed_attributes
        self.allowed_children = allowed_children
        if self.tag == "MediaContainer":
            if self.encoding == "xml":
                self.set_content("application/xml")
            else:
                self.set_content("application/json")

    def Content(self):
        if self.encoding == "xml":
            encoded = self.to_xml()
            return encoded
        else:
            encoded = self.to_json()
            return encoded

    def add(self, obj):
        self.children.append(obj)
        new_string = obj.to_xml()
        self.child_strings.append(new_string)

    def set(self, key, value):
        if self.attributes is None:
            self.attributes = {}
        self.attributes[key] = value

    def get(self, key):
        if self.attributes is not None:
            return self.attributes.get(key) or None
        return None

    def size(self):
        if self.children is None:
            return 0
        else:
            return len(self.children)


    def to_xml(self):
        s = escape(str(self.tag))
        self_tag = s[0].upper() + s[1:]

        self_attributes = self.attributes
        if self.container_size:
            container_max = self.container_start + self.container_size
            if len(self.child_strings) > container_max:
                self.child_strings = self.child_strings[self.container_start:container_max]

        child_strings = self.child_strings

        attribute_string = ""
        if self_attributes is not None:
            for key in self_attributes:
                allowed = True
                if self.allowed_attributes is not None:
                    allowed = False
                    if key in self.allowed_attributes:
                        allowed = True

                value = self_attributes.get(key)
                if allowed:
                    if value not in [None, ""]:
                        if type(value) == dict:
                            child_strings.append(self.child_xml(key, value))
                        elif type(value) == list:
                            for child_dict in value:
                                child_strings.append(self.child_xml(key, child_dict))
                        else:
                            if type(value) == str:
                                value = quoteattr(value)
                                attribute_string += ' %s=%s' % (escape(str(key)), value)
                            else:
                                attribute_string += ' %s="%s"' % (escape(str(key)), value)
                else:
                    Log.error("Attribute " + key + " is not allowed in this container.")

        if self.show_size is True:
            attribute_string += ' %s="%s"' % ("size", len(child_strings))

        if self_tag == "MediaContainer":
            attribute_string += ' version="%s"' % "1.1.107"

        if len(child_strings) == 0:
            string = "<%s%s/>" % (self_tag, attribute_string)
        else:
            child_string = "\n".join(child_strings)
            string = "<%s%s>%s</%s>\n" % (self_tag, attribute_string, child_string, self_tag)

        return string

    def set_content(self, value):
        response.content_type = value

    def to_json(self):

        json_obj = self.attributes
        if json_obj is None:
            json_obj = {}
        self_size = 0
        if self.show_size is True:
            if "size" in json_obj:
                json_obj["oldSize"] = json_obj["size"]
            if self.children is not None:
                if self.container_size:
                    container_max = self.container_start + self.container_size
                    if len(self.children) > container_max:
                        self.children = self.children[self.container_start:container_max]
                self_size = len(self.children)
            json_obj["size"] = self_size

        for child in self.children:
            child_dict = child.to_json()
            (key, value) = child_dict
            child_list = json_obj.get(key) or []
            child_list.append(value)
            json_obj[key] = child_list

        if self.tag == "MediaContainer":
            result = {
                self.tag: json_obj
            }
            if self.show_size:
                result['size'] = self_size
            json_string = json.dumps(result, sort_keys=False, indent=4, separators=(',', ': '))

            return json_string
        else:
            s = str(self.tag)
            self_tag = s[0].upper() + s[1:]
            Log.debug("Appending child %s " % self_tag)
            result = (self_tag, json_obj)

            return result

    def child_xml(self, tag, data):
        children = []
        attributes = []
        for key, value in data.items():
            if type(value) == dict:
                children.append(self.child_xml(key, value))
            elif type(value) == list:
                for item in value:
                    if type(item) == dict:
                        item_attributes = []
                        for sub_key, sub_value in item.items():
                            item_str = '%s=%s' % (escape(str(sub_key)), quoteattr(sub_value))
                            item_attributes.append(item_str)
                        children.append("<%s %s/>" % (escape(str(key)), " ".join(item_attributes)))
            else:
                if type(value) == str:
                    value = quoteattr(value)
                else:
                    value = '"%s"' % value
                item_str = '%s=%s' % (escape(str(key)), value)
                attributes.append(item_str)

        if len(children):
            if len(attributes):
                output = "<%s %s>%s</%s>" % (tag, " ".join(attributes), "\n".join(children), tag)
            else:
                output = "<%s>%s</%s>" % (tag, "\n".join(children), tag)
        else:
            if len(attributes):
                output = "<%s %s/>" % (tag, " ".join(attributes))
            else:
                output = "<%s/> % tag"
        return output


