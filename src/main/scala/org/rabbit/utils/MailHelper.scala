package org.rabbit.utils

import org.apache.commons.mail.{Email, EmailAttachment, HtmlEmail, MultiPartEmail, SimpleEmail}
import org.rabbit.config.FileConfig

sealed abstract class MailType

case object Plain extends MailType

case object Rich extends MailType

case object MultiPart extends MailType

case class MailInfo(
                     to: Seq[String],
                     cc: Seq[String] = Seq.empty, //抄送地址
                     bcc: Seq[String] = Seq.empty, //密件抄送地址
                     subject: String,
                     message: String,
                     richMessage: Option[String] = None,
                     attachment: Option[(java.io.File)] = None
                   )

object MailHelper {

  def main(args: Array[String]): Unit = {
    MailHelper.send( MailInfo (
      to = "ispmd@foxmail.com":: Nil,
      subject = "flink job 任务监控",
      message = "hi flink"
    ))
  }



  def send(emailInfo: MailInfo) {


    val format =
      if (emailInfo.attachment.isDefined) MultiPart
      else if (emailInfo.richMessage.isDefined) Rich
      else Plain

    val email: Email = format match {
      case Plain => new SimpleEmail().setMsg(emailInfo.message)
      case Rich => new HtmlEmail().setHtmlMsg(emailInfo.richMessage.get).setTextMsg(emailInfo.message)
      case MultiPart => {
        val attachment = new EmailAttachment()
        attachment.setPath(emailInfo.attachment.get.getAbsolutePath)
        attachment.setDisposition(EmailAttachment.ATTACHMENT)
        attachment.setName(emailInfo.attachment.get.getName)
        new MultiPartEmail().attach(attachment).setMsg(emailInfo.message)
      }
    }

    val hostName = FileConfig.emailConfig.getString("hostName")
    val userName = FileConfig.emailConfig.getString("userName")
    val password = FileConfig.emailConfig.getString("password")
    email.setHostName(hostName)
    email.setAuthentication(userName, password)
    email.setFrom(userName)
    email.setCharset("UTF-8")
    email.setSSLOnConnect(true)

    emailInfo.to foreach email.addTo

    email
      .setSubject(emailInfo.subject)
      .setMsg(emailInfo.message)
      .send()
  }

}
