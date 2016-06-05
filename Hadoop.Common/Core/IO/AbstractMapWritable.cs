using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Apache.NMS.Util;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
    /// <summary>
    /// Abstract base class for MapWritable and SortedMapWritable
    /// Unlike org.apache.nutch.crawl.MapWritable, this class allows creation of
    /// MapWritable&lt;Writable, MapWritable&gt; so the CLASS_TO_ID and ID_TO_CLASS
    /// maps travel with the class instead of being static.
    /// </summary>
    /// <remarks>
    /// Abstract base class for MapWritable and SortedMapWritable
    /// Unlike org.apache.nutch.crawl.MapWritable, this class allows creation of
    /// MapWritable&lt;Writable, MapWritable&gt; so the CLASS_TO_ID and ID_TO_CLASS
    /// maps travel with the class instead of being static.
    /// Class ids range from 1 to 127 so there can be at most 127 distinct classes
    /// in any specific map instance.
    /// </remarks>
    
    public abstract class AbstractMapWritable : IWritable, Configurable
	{
		private AtomicReference<Configuration> conf;

		
		internal IDictionary<Type, byte> classToIdMap = new ConcurrentDictionary<Type, byte>();

		
		internal IDictionary<byte, Type> idToClassMap = new ConcurrentDictionary<byte, Type>();

		private volatile byte _newClasses = 0;
        private readonly object _lockObj;

        /* Class to id mappings */
		/* Id to Class mappings */
		/* The number of new classes (those not established by the constructor) */
		/// <returns>the number of known classes</returns>
		internal virtual byte GetNewClasses()
		{
			return _newClasses;
		}

		/// <summary>Used to add "predefined" classes and by Writable to copy "new" classes.</summary>
		private void AddToMap(Type clazz, byte id)
		{
			lock (_lockObj)
			{
				if (classToIdMap.ContainsKey(clazz))
				{
					byte b = classToIdMap[clazz];
					if (b != id)
					{
						throw new ArgumentException("Class " + clazz.FullName + " already registered but maps to "
							 + b + " and not " + id);
					}
				}
				if (idToClassMap.ContainsKey(id))
				{
					Type c = idToClassMap[id];
					if (!(c == clazz))
					{
						throw new ArgumentException("Id " + id + " exists but maps to " + c.FullName + " and not "
							 + clazz.FullName);
					}
				}
				classToIdMap[clazz] = id;
				idToClassMap[id] = clazz;
			}
		}

		/// <summary>Add a Class to the maps if it is not already present.</summary>
		protected internal virtual void AddToMap(Type clazz)
		{
			lock (this)
			{
				if (classToIdMap.ContainsKey(clazz))
				{
					return;
				}
				if (_newClasses + 1 > byte.MaxValue)
				{
					throw new IndexOutOfRangeException("adding an additional class would" + " exceed the maximum number allowed"
						);
				}
				byte id = ++_newClasses;
				AddToMap(clazz, id);
			}
		}

		/// <returns>the Class class for the specified id</returns>
		protected internal virtual Type GetClass(byte id)
		{
			return idToClassMap[id];
		}

		/// <returns>the id for the specified Class</returns>
		protected internal virtual byte GetId(Type clazz)
		{
			return (byte) (classToIdMap.ContainsKey(clazz) ? classToIdMap[clazz] : -1);
		}

		/// <summary>Used by child copy constructors.</summary>
		protected internal virtual void Copy(IWritable other)
		{
			lock (this)
			{
				if (other != null)
				{
					try
					{
					    var memoryStream = new MemoryStream();
					    var binaryWriter = new BinaryWriter(memoryStream);
					    DataOutputBuffer outputBuffer = new DataOutputBuffer();
						other.Write(binaryWriter);
                        
                        DataInputBuffer inputBuffer = new DataInputBuffer();
						inputBuffer.Reset(outputBuffer.GetData(), outputBuffer.GetLength());
						ReadFields(inputBuffer);
					}
					catch (IOException e)
					{
						throw new ArgumentException("map cannot be copied: " + e.Message);
					}
				}
				else
				{
					throw new ArgumentException("source map cannot be null");
				}
			}
		}

		/// <summary>constructor.</summary>
		protected internal AbstractMapWritable(object lockObj)
		{
		    _lockObj = lockObj;
		    this.conf = new AtomicReference<Configuration>();
			
			AddToMap(typeof(ArrayWritable), Convert.ToByte(-127));
			AddToMap(typeof(BooleanWritable), Convert.ToByte(-126));
			AddToMap(typeof(BytesWritable), Convert.ToByte(-125));
			AddToMap(typeof(FloatWritable), Convert.ToByte(-124));
			AddToMap(typeof(IntWritable), Convert.ToByte(-123));
			AddToMap(typeof(LongWritable), Convert.ToByte(-122));
			AddToMap(typeof(MapWritable), Convert.ToByte(-121));
			AddToMap(typeof(MD5Hash), Convert.ToByte(-120));
			AddToMap(typeof(NullWritable), Convert.ToByte(-119));
			AddToMap(typeof(ObjectWritable), Convert.ToByte(-118));
			AddToMap(typeof(SortedMapWritable), Convert.ToByte(-117));
			AddToMap(typeof(Text), Convert.ToByte(-116));
			AddToMap(typeof(TwoDArrayWritable), Convert.ToByte(-115));
			// UTF8 is deprecated so we don't support it
			AddToMap(typeof(VIntWritable), Convert.ToByte(-114));
			AddToMap(typeof(VLongWritable), Convert.ToByte(-113));
		}

		/// <returns>the conf</returns>
		public virtual Configuration GetConf()
		{
			return conf.Value;
		}

		/// <param name="conf">the conf to set</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf.Value = (conf);
		}
        
		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter @out)
		{
			// First write out the size of the class table and any classes that are
			// "unknown" classes
			@out.WriteByte(_newClasses);
			for (byte i = 1; ((sbyte)i) <= _newClasses; i++)
			{
				@out.WriteByte(i);
				@out.WriteUTF(GetClass(i).FullName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader @in)
		{
			// Get the number of "unknown" classes
			_newClasses = @in.ReadByte();
			// Then read in the class names and add them to our tables
			for (int i = 0; i < _newClasses; i++)
			{
				byte id = @in.ReadByte();
				string className = @in.ReadUTF();
				try
				{
					AddToMap(Sharpen.Runtime.GetType(className), id);
				}
				catch (TypeLoadException e)
				{
					throw new IOException("can't find class: " + className + " because " + e.Message);
				}
			}
		}
	}
}
