using System;
using System.Collections.Generic;
using System.IO;
using Com.Google.Common.Annotations;
using Org.Apache.Hadoop.Conf;
using Sharpen;

namespace Org.Apache.Hadoop.IO
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
	public abstract class AbstractMapWritable : Writable, Configurable
	{
		private AtomicReference<Configuration> conf;

		[VisibleForTesting]
		internal IDictionary<Type, byte> classToIdMap = new ConcurrentHashMap<Type, byte>
			();

		[VisibleForTesting]
		internal IDictionary<byte, Type> idToClassMap = new ConcurrentHashMap<byte, Type>
			();

		private volatile byte newClasses = 0;

		/* Class to id mappings */
		/* Id to Class mappings */
		/* The number of new classes (those not established by the constructor) */
		/// <returns>the number of known classes</returns>
		internal virtual byte GetNewClasses()
		{
			return newClasses;
		}

		/// <summary>Used to add "predefined" classes and by Writable to copy "new" classes.</summary>
		private void AddToMap(Type clazz, byte id)
		{
			lock (this)
			{
				if (classToIdMap.Contains(clazz))
				{
					byte b = classToIdMap[clazz];
					if (b != id)
					{
						throw new ArgumentException("Class " + clazz.FullName + " already registered but maps to "
							 + b + " and not " + id);
					}
				}
				if (idToClassMap.Contains(id))
				{
					Type c = idToClassMap[id];
					if (!c.Equals(clazz))
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
				if (classToIdMap.Contains(clazz))
				{
					return;
				}
				if (newClasses + 1 > byte.MaxValue)
				{
					throw new IndexOutOfRangeException("adding an additional class would" + " exceed the maximum number allowed"
						);
				}
				byte id = ++newClasses;
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
			return classToIdMap.Contains(clazz) ? classToIdMap[clazz] : -1;
		}

		/// <summary>Used by child copy constructors.</summary>
		protected internal virtual void Copy(Writable other)
		{
			lock (this)
			{
				if (other != null)
				{
					try
					{
						DataOutputBuffer @out = new DataOutputBuffer();
						other.Write(@out);
						DataInputBuffer @in = new DataInputBuffer();
						@in.Reset(@out.GetData(), @out.GetLength());
						ReadFields(@in);
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
		protected internal AbstractMapWritable()
		{
			this.conf = new AtomicReference<Configuration>();
			AddToMap(typeof(ArrayWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-127)));
			AddToMap(typeof(BooleanWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-126)));
			AddToMap(typeof(BytesWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-125)));
			AddToMap(typeof(FloatWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-124)));
			AddToMap(typeof(IntWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-123)));
			AddToMap(typeof(LongWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-122)));
			AddToMap(typeof(MapWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-121)));
			AddToMap(typeof(MD5Hash), byte.ValueOf(Sharpen.Extensions.ValueOf(-120)));
			AddToMap(typeof(NullWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-119)));
			AddToMap(typeof(ObjectWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-118)));
			AddToMap(typeof(SortedMapWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-117)
				));
			AddToMap(typeof(Text), byte.ValueOf(Sharpen.Extensions.ValueOf(-116)));
			AddToMap(typeof(TwoDArrayWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-115)
				));
			// UTF8 is deprecated so we don't support it
			AddToMap(typeof(VIntWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-114)));
			AddToMap(typeof(VLongWritable), byte.ValueOf(Sharpen.Extensions.ValueOf(-113)));
		}

		/// <returns>the conf</returns>
		public virtual Configuration GetConf()
		{
			return conf.Get();
		}

		/// <param name="conf">the conf to set</param>
		public virtual void SetConf(Configuration conf)
		{
			this.conf.Set(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(DataOutput @out)
		{
			// First write out the size of the class table and any classes that are
			// "unknown" classes
			@out.WriteByte(newClasses);
			for (byte i = 1; ((sbyte)i) <= newClasses; i++)
			{
				@out.WriteByte(i);
				@out.WriteUTF(GetClass(i).FullName);
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(DataInput @in)
		{
			// Get the number of "unknown" classes
			newClasses = @in.ReadByte();
			// Then read in the class names and add them to our tables
			for (int i = 0; i < newClasses; i++)
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
