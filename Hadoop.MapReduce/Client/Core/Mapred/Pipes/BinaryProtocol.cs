using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using Org.Apache.Commons.Logging;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapred.Pipes
{
	/// <summary>This protocol is a binary implementation of the Pipes protocol.</summary>
	internal class BinaryProtocol<K1, V1, K2, V2> : DownwardProtocol<K1, V1>
		where K1 : WritableComparable
		where V1 : Writable
		where K2 : WritableComparable
		where V2 : Writable
	{
		public const int CurrentProtocolVersion = 0;

		/// <summary>The buffer size for the command socket</summary>
		private const int BufferSize = 128 * 1024;

		private DataOutputStream stream;

		private DataOutputBuffer buffer = new DataOutputBuffer();

		private static readonly Log Log = LogFactory.GetLog(typeof(Org.Apache.Hadoop.Mapred.Pipes.BinaryProtocol
			).FullName);

		private BinaryProtocol.UplinkReaderThread uplink;

		/// <summary>The integer codes to represent the different messages.</summary>
		/// <remarks>
		/// The integer codes to represent the different messages. These must match
		/// the C++ codes or massive confusion will result.
		/// </remarks>
		[System.Serializable]
		private sealed class MessageType
		{
			public static readonly BinaryProtocol.MessageType Start = new BinaryProtocol.MessageType
				(0);

			public static readonly BinaryProtocol.MessageType SetJobConf = new BinaryProtocol.MessageType
				(1);

			public static readonly BinaryProtocol.MessageType SetInputTypes = new BinaryProtocol.MessageType
				(2);

			public static readonly BinaryProtocol.MessageType RunMap = new BinaryProtocol.MessageType
				(3);

			public static readonly BinaryProtocol.MessageType MapItem = new BinaryProtocol.MessageType
				(4);

			public static readonly BinaryProtocol.MessageType RunReduce = new BinaryProtocol.MessageType
				(5);

			public static readonly BinaryProtocol.MessageType ReduceKey = new BinaryProtocol.MessageType
				(6);

			public static readonly BinaryProtocol.MessageType ReduceValue = new BinaryProtocol.MessageType
				(7);

			public static readonly BinaryProtocol.MessageType Close = new BinaryProtocol.MessageType
				(8);

			public static readonly BinaryProtocol.MessageType Abort = new BinaryProtocol.MessageType
				(9);

			public static readonly BinaryProtocol.MessageType AuthenticationReq = new BinaryProtocol.MessageType
				(10);

			public static readonly BinaryProtocol.MessageType Output = new BinaryProtocol.MessageType
				(50);

			public static readonly BinaryProtocol.MessageType PartitionedOutput = new BinaryProtocol.MessageType
				(51);

			public static readonly BinaryProtocol.MessageType Status = new BinaryProtocol.MessageType
				(52);

			public static readonly BinaryProtocol.MessageType Progress = new BinaryProtocol.MessageType
				(53);

			public static readonly BinaryProtocol.MessageType Done = new BinaryProtocol.MessageType
				(54);

			public static readonly BinaryProtocol.MessageType RegisterCounter = new BinaryProtocol.MessageType
				(55);

			public static readonly BinaryProtocol.MessageType IncrementCounter = new BinaryProtocol.MessageType
				(56);

			public static readonly BinaryProtocol.MessageType AuthenticationResp = new BinaryProtocol.MessageType
				(57);

			internal readonly int code;

			internal MessageType(int code)
			{
				this.code = code;
			}
		}

		private class UplinkReaderThread<K2, V2> : Sharpen.Thread
			where K2 : WritableComparable
			where V2 : Writable
		{
			private DataInputStream inStream;

			private UpwardProtocol<K2, V2> handler;

			private K2 key;

			private V2 value;

			private bool authPending = true;

			/// <exception cref="System.IO.IOException"/>
			public UplinkReaderThread(InputStream stream, UpwardProtocol<K2, V2> handler, K2 
				key, V2 value)
			{
				inStream = new DataInputStream(new BufferedInputStream(stream, BufferSize));
				this.handler = handler;
				this.key = key;
				this.value = value;
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void CloseConnection()
			{
				inStream.Close();
			}

			public override void Run()
			{
				while (true)
				{
					try
					{
						if (Sharpen.Thread.CurrentThread().IsInterrupted())
						{
							throw new Exception();
						}
						int cmd = WritableUtils.ReadVInt(inStream);
						Log.Debug("Handling uplink command " + cmd);
						if (cmd == BinaryProtocol.MessageType.AuthenticationResp.code)
						{
							string digest = Text.ReadString(inStream);
							authPending = !handler.Authenticate(digest);
						}
						else
						{
							if (authPending)
							{
								Log.Warn("Message " + cmd + " received before authentication is " + "complete. Ignoring"
									);
								continue;
							}
							else
							{
								if (cmd == BinaryProtocol.MessageType.Output.code)
								{
									ReadObject(key);
									ReadObject(value);
									handler.Output(key, value);
								}
								else
								{
									if (cmd == BinaryProtocol.MessageType.PartitionedOutput.code)
									{
										int part = WritableUtils.ReadVInt(inStream);
										ReadObject(key);
										ReadObject(value);
										handler.PartitionedOutput(part, key, value);
									}
									else
									{
										if (cmd == BinaryProtocol.MessageType.Status.code)
										{
											handler.Status(Text.ReadString(inStream));
										}
										else
										{
											if (cmd == BinaryProtocol.MessageType.Progress.code)
											{
												handler.Progress(inStream.ReadFloat());
											}
											else
											{
												if (cmd == BinaryProtocol.MessageType.RegisterCounter.code)
												{
													int id = WritableUtils.ReadVInt(inStream);
													string group = Text.ReadString(inStream);
													string name = Text.ReadString(inStream);
													handler.RegisterCounter(id, group, name);
												}
												else
												{
													if (cmd == BinaryProtocol.MessageType.IncrementCounter.code)
													{
														int id = WritableUtils.ReadVInt(inStream);
														long amount = WritableUtils.ReadVLong(inStream);
														handler.IncrementCounter(id, amount);
													}
													else
													{
														if (cmd == BinaryProtocol.MessageType.Done.code)
														{
															Log.Debug("Pipe child done");
															handler.Done();
															return;
														}
														else
														{
															throw new IOException("Bad command code: " + cmd);
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
					catch (Exception)
					{
						return;
					}
					catch (Exception e)
					{
						Log.Error(StringUtils.StringifyException(e));
						handler.Failed(e);
						return;
					}
				}
			}

			/// <exception cref="System.IO.IOException"/>
			private void ReadObject(Writable obj)
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
		}

		/// <summary>An output stream that will save a copy of the data into a file.</summary>
		private class TeeOutputStream : FilterOutputStream
		{
			private OutputStream file;

			/// <exception cref="System.IO.IOException"/>
			internal TeeOutputStream(string filename, OutputStream @base)
				: base(@base)
			{
				file = new FileOutputStream(filename);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(byte[] b, int off, int len)
			{
				file.Write(b, off, len);
				@out.Write(b, off, len);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Write(int b)
			{
				file.Write(b);
				@out.Write(b);
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Flush()
			{
				file.Flush();
				@out.Flush();
			}

			/// <exception cref="System.IO.IOException"/>
			public override void Close()
			{
				Flush();
				file.Close();
				@out.Close();
			}
		}

		/// <summary>Create a proxy object that will speak the binary protocol on a socket.</summary>
		/// <remarks>
		/// Create a proxy object that will speak the binary protocol on a socket.
		/// Upward messages are passed on the specified handler and downward
		/// downward messages are public methods on this object.
		/// </remarks>
		/// <param name="sock">The socket to communicate on.</param>
		/// <param name="handler">The handler for the received messages.</param>
		/// <param name="key">The object to read keys into.</param>
		/// <param name="value">The object to read values into.</param>
		/// <param name="config">The job's configuration</param>
		/// <exception cref="System.IO.IOException"/>
		public BinaryProtocol(Socket sock, UpwardProtocol<K2, V2> handler, K2 key, V2 value
			, JobConf config)
		{
			OutputStream raw = sock.GetOutputStream();
			// If we are debugging, save a copy of the downlink commands to a file
			if (Submitter.GetKeepCommandFile(config))
			{
				raw = new BinaryProtocol.TeeOutputStream("downlink.data", raw);
			}
			stream = new DataOutputStream(new BufferedOutputStream(raw, BufferSize));
			uplink = new BinaryProtocol.UplinkReaderThread<K2, V2>(sock.GetInputStream(), handler
				, key, value);
			uplink.SetName("pipe-uplink-handler");
			uplink.Start();
		}

		/// <summary>Close the connection and shutdown the handler thread.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public virtual void Close()
		{
			Log.Debug("closing connection");
			stream.Close();
			uplink.CloseConnection();
			uplink.Interrupt();
			uplink.Join();
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Authenticate(string digest, string challenge)
		{
			Log.Debug("Sending AUTHENTICATION_REQ, digest=" + digest + ", challenge=" + challenge
				);
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.AuthenticationReq.code
				);
			Text.WriteString(stream, digest);
			Text.WriteString(stream, challenge);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Start()
		{
			Log.Debug("starting downlink");
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.Start.code);
			WritableUtils.WriteVInt(stream, CurrentProtocolVersion);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetJobConf(JobConf job)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.SetJobConf.code);
			IList<string> list = new AList<string>();
			foreach (KeyValuePair<string, string> itm in job)
			{
				list.AddItem(itm.Key);
				list.AddItem(itm.Value);
			}
			WritableUtils.WriteVInt(stream, list.Count);
			foreach (string entry in list)
			{
				Text.WriteString(stream, entry);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void SetInputTypes(string keyType, string valueType)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.SetInputTypes.code);
			Text.WriteString(stream, keyType);
			Text.WriteString(stream, valueType);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RunMap(InputSplit split, int numReduces, bool pipedInput)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.RunMap.code);
			WriteObject(split);
			WritableUtils.WriteVInt(stream, numReduces);
			WritableUtils.WriteVInt(stream, pipedInput ? 1 : 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void MapItem(WritableComparable key, Writable value)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.MapItem.code);
			WriteObject(key);
			WriteObject(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void RunReduce(int reduce, bool pipedOutput)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.RunReduce.code);
			WritableUtils.WriteVInt(stream, reduce);
			WritableUtils.WriteVInt(stream, pipedOutput ? 1 : 0);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReduceKey(WritableComparable key)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.ReduceKey.code);
			WriteObject(key);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReduceValue(Writable value)
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.ReduceValue.code);
			WriteObject(value);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void EndOfInput()
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.Close.code);
			Log.Debug("Sent close command");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Abort()
		{
			WritableUtils.WriteVInt(stream, BinaryProtocol.MessageType.Abort.code);
			Log.Debug("Sent abort command");
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Flush()
		{
			stream.Flush();
		}

		/// <summary>Write the given object to the stream.</summary>
		/// <remarks>
		/// Write the given object to the stream. If it is a Text or BytesWritable,
		/// write it directly. Otherwise, write it to a buffer and then write the
		/// length and data to the stream.
		/// </remarks>
		/// <param name="obj">the object to write</param>
		/// <exception cref="System.IO.IOException"/>
		private void WriteObject(Writable obj)
		{
			// For Text and BytesWritable, encode them directly, so that they end up
			// in C++ as the natural translations.
			if (obj is Text)
			{
				Text t = (Text)obj;
				int len = t.GetLength();
				WritableUtils.WriteVInt(stream, len);
				stream.Write(t.GetBytes(), 0, len);
			}
			else
			{
				if (obj is BytesWritable)
				{
					BytesWritable b = (BytesWritable)obj;
					int len = b.GetLength();
					WritableUtils.WriteVInt(stream, len);
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
		}
	}
}
