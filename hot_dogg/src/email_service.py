import smtplib
import logging
import asyncio
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class EmailService:
    """
    Service for sending email alerts
    """
    
    def __init__(self, smtp_server, smtp_port, username, password):
        """
        Initialize the email service
        
        Args:
            smtp_server: SMTP server address
            smtp_port: SMTP server port
            username: Email username/address
            password: Email password
        """
        self.smtp_server = smtp_server
        self.smtp_port = int(smtp_port)
        self.username = username
        self.password = password
        self.logger = logging.getLogger(__name__)
        
    async def send_email(self, subject, body, to_email):
        """
        Send an email to one or multiple recipients
        
        Args:
            subject: Email subject
            body: Email body
            to_email: Recipient email address(es) - can be a string or comma-separated string
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Handle multiple email addresses
            if isinstance(to_email, str):
                email_list = [email.strip() for email in to_email.split(',') if email.strip()]
            else:
                email_list = [to_email]
            
            # Create a multipart message
            message = MIMEMultipart()
            message["From"] = self.username
            message["To"] = ", ".join(email_list)  # Properly format multiple recipients
            message["Subject"] = subject
            
            # Add body to email
            message.attach(MIMEText(body, "plain"))
            
            # Use asyncio to run SMTP sending in a thread pool
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self._send_email_sync, message, email_list)
            
            self.logger.info(f"Alert sent: {subject}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send email: {str(e)}")
            return False
            
    def _send_email_sync(self, message, email_list):
        """
        Synchronous function to send email (to be run in thread pool)
        """
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.starttls()
            server.login(self.username, self.password)
            server.sendmail(self.username, email_list, message.as_string())
