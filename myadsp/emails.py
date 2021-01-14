"""email templates"""

from builtins import object
class Email(object):
    """
    Data structure that contains email content data
    """
    msg_plain = """{payload}"""
    msg_html = """{payload}"""
    subject = u''
    salt = ''


class myADSTemplate(Email):
    """
    myADS email template
    """

    msg_plain = """
        SAO/NASA ADS: myADS Personal Notification Service Results
    
        {payload}
        """
    msg_html = """{payload}"""
    subject = u'myADS Notification'
