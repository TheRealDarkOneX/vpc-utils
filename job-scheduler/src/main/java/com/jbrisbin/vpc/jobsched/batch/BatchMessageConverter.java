package com.jbrisbin.vpc.jobsched.batch;

import com.jbrisbin.vpc.jobsched.SecureMessageConverter;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.support.converter.MessageConversionException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * @author Jon Brisbin <jon@jbrisbin.com>
 */
public class BatchMessageConverter extends SecureMessageConverter {

  public Message toMessage(Object object,
                           MessageProperties props) throws MessageConversionException {
    if (object instanceof BatchMessage) {
      BatchMessage batch = (BatchMessage) object;
      props.setCorrelationId(batch.getId().getBytes());
      props.setContentType("application/zip");

      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      ZipOutputStream zout = new ZipOutputStream(bout);
      for (Map.Entry<String, String> msg : batch.getMessages().entrySet()) {
        ZipEntry zentry = new ZipEntry(msg.getKey());
        try {
          zout.putNextEntry(zentry);
          zout.write(msg.getValue().getBytes());
          zout.closeEntry();
        } catch (IOException e) {
          throw new MessageConversionException(e.getMessage(), e);
        }
      }

      try {
        zout.flush();
        zout.close();
      } catch (IOException e) {
        throw new MessageConversionException(e.getMessage(), e);
      }

      return new Message(bout.toByteArray(), props);
    } else {
      throw new MessageConversionException(
          "Cannot convert object " + String.valueOf(object) + " using " + getClass()
              .toString());
    }
  }

  public Object fromMessage(Message message) throws MessageConversionException {
    MessageProperties props = message.getMessageProperties();
    if (isAuthorized(props) && props.getContentType().equals("application/zip")) {
      BatchMessage msg = new BatchMessage();
      msg.setId(new String(props.getCorrelationId()));
      String timeout = "2";
      if (props.getHeaders().containsKey("timeout")) {
        timeout = props.getHeaders().get("timeout").toString();
      }
      msg.setTimeout(Integer.parseInt(timeout));

      ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(message.getBody()));
      ZipEntry zentry;
      try {
        while (null != (zentry = zin.getNextEntry())) {
          byte[] buff = new byte[4096];
          StringWriter json = new StringWriter();
          for (int bytesRead = 0; bytesRead > -1; bytesRead = zin.read(buff)) {
            json.write(new String(buff, 0, bytesRead));
          }
          msg.getMessages().put(zentry.getName(), json.toString());
        }
      } catch (IOException e) {
        throw new MessageConversionException(e.getMessage(), e);
      }

      return msg;
    } else {
      throw new MessageConversionException("Invalid security key.");
    }
  }

}
