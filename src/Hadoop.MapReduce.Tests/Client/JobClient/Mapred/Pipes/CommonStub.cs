using System.IO;
using System.Net;
using System.Net.Sockets;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapreduce.Security;
using Org.Apache.Hadoop.Mapreduce.Security.Token;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	public class CommonStub
	{
		protected internal Socket socket = null;

		protected internal DataInputStream dataInput;

		protected internal DataOutputStream dataOut;

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual string CreateDigest(byte[] password, string data)
		{
			SecretKey key = JobTokenSecretManager.CreateSecretKey(password);
			return SecureShuffleUtils.HashFromString(data, key);
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void ReadObject(Writable obj, DataInputStream inStream
			)
		{
			int numBytes = WritableUtils.ReadVInt(inStream);
			byte[] buffer;
			// For BytesWritable and Text, use the specified length to set the length
			// this causes the "obvious" translations to work. So that if you emit
			// a string "abc" from C++, it shows up as "abc".
			if (obj is BytesWritable)
			{
				buffer = new byte[numBytes];
				inStream.ReadFully(buffer);
				((BytesWritable)obj).Set(buffer, 0, numBytes);
			}
			else
			{
				if (obj is Text)
				{
					buffer = new byte[numBytes];
					inStream.ReadFully(buffer);
					((Text)obj).Set(buffer);
				}
				else
				{
					obj.ReadFields(inStream);
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		protected internal virtual void WriteObject(Writable obj, DataOutputStream stream
			)
		{
			// For Text and BytesWritable, encode them directly, so that they end up
			// in C++ as the natural translations.
			DataOutputBuffer buffer = new DataOutputBuffer();
			if (obj is Text)
			{
				Text t = (Text)obj;
				int len = t.GetLength();
				WritableUtils.WriteVLong(stream, len);
				stream.Flush();
				stream.Write(t.GetBytes(), 0, len);
				stream.Flush();
			}
			else
			{
				if (obj is BytesWritable)
				{
					BytesWritable b = (BytesWritable)obj;
					int len = b.GetLength();
					WritableUtils.WriteVLong(stream, len);
					stream.Write(b.GetBytes(), 0, len);
				}
				else
				{
					buffer.Reset();
					obj.Write(buffer);
					int length = buffer.GetLength();
					WritableUtils.WriteVInt(stream, length);
					stream.Write(buffer.GetData(), 0, length);
				}
			}
			stream.Flush();
		}

		/// <exception cref="System.Exception"/>
		protected internal virtual void InitSoket()
		{
			int port = System.Convert.ToInt32(Runtime.Getenv("mapreduce.pipes.command.port"));
			IPAddress address = Sharpen.Runtime.GetLocalHost();
			socket = Sharpen.Extensions.CreateSocket(address.GetHostName(), port);
			InputStream input = socket.GetInputStream();
			OutputStream output = socket.GetOutputStream();
			// try to read
			dataInput = new DataInputStream(input);
			WritableUtils.ReadVInt(dataInput);
			string str = Text.ReadString(dataInput);
			Text.ReadString(dataInput);
			dataOut = new DataOutputStream(output);
			WritableUtils.WriteVInt(dataOut, 57);
			string s = CreateDigest(Sharpen.Runtime.GetBytesForString("password"), str);
			Text.WriteString(dataOut, s);
			// start
			WritableUtils.ReadVInt(dataInput);
			int cuttentAnswer = WritableUtils.ReadVInt(dataInput);
			System.Console.Out.WriteLine("CURRENT_PROTOCOL_VERSION:" + cuttentAnswer);
			// get configuration
			// should be MessageType.SET_JOB_CONF.code
			WritableUtils.ReadVInt(dataInput);
			// array length
			int j = WritableUtils.ReadVInt(dataInput);
			for (int i = 0; i < j; i++)
			{
				Text.ReadString(dataInput);
				i++;
				Text.ReadString(dataInput);
			}
		}

		protected internal virtual void CloseSoket()
		{
			if (socket != null)
			{
				try
				{
					socket.Close();
				}
				catch (IOException e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
				}
			}
		}
	}
}
