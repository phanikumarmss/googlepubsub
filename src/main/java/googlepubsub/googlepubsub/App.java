package googlepubsub.googlepubsub;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;

/**
 * Hello world!
 *
 */
public class App 
{
	static final String PROJECT_ID = "pubsub-1";
	static TopicName topicName = TopicName.create(PROJECT_ID, "testing");
    public static void main( String[] args )
    {
    	publishmessage();
    }
    public static void publishmessage()
    {
    	try
    	{
	    	Publisher publisher = null;
			System.out.println("Welcome");
			List<ApiFuture<String>> apiFutures = new ArrayList();
			System.out.println("Second");
			publisher = Publisher.defaultBuilder(topicName).build();
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\Prasad\\Desktop\\pubsub\\sample_message1.json"));
			try
			{
				System.out.println("Inside");
				String message1 = "";
				while((message1=br.readLine())!=null)
				{
					Thread.sleep(100);
					ApiFuture<String> messageId = publishMessage(publisher, message1);
					apiFutures.add(messageId);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			finally 
			{
				System.out.println("Entered Finally");
				List<String> messageIds = ApiFutures.allAsList(apiFutures).get();
				for (String messageId : messageIds) 
				{
					System.out.println(messageId);
				}
				if (publisher != null) 
				{
					publisher.shutdown();
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Welcome"+e.getMessage()+"\n");
			e.printStackTrace();
		}
    }
    private static ApiFuture<String> publishMessage(Publisher publisher, String message)
  	      throws Exception 
  	{
  	    // convert message to bytes
  	    ByteString data = ByteString.copyFromUtf8(message);
  	    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
  	    return publisher.publish(pubsubMessage);
  	  }
}
