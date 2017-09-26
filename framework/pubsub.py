# encoding: UTF-8

from abc import abstractmethod


########################################################################
class Publisher(object):

    #----------------------------------------------------------------------
    def __init__(self):
        self.subscribers = {}

    
    def onSubscribe(self, subscriber, topic):
        if (self.subscribers.has_key(topic)):
            self.subscribers.get(topic).append(subscriber)
        else:
            self.subscribers[topic] = []
            self.subscribers.get(topic).append(subscriber)
            
    def publish(self, event, topic):
        if (self.subscribers.has_key(topic)):
            sublist = self.subscribers.get(topic)
            for i in xrange(len(sublist)):
                sublist[i].onEvent(event, topic)
    
    def getTopic(self):
        return self.subscribers.keys()
    
                
class Subscriber(object):
    
    #----------------------------------------------------------------------
    def __init__(self):
        pass
    
    @abstractmethod
    def subscribe(self, publisher, topic):
        pass
    
    @abstractmethod
    def onEvent(self, event, topic):
        pass
    
    