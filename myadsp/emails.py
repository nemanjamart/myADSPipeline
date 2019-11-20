"""email templates"""

class Email(object):
    """
    Data structure that contains email content data
    """
    msg_plain = ''
    msg_html = ''
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
