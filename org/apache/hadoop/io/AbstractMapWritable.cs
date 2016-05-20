using Sharpen;

namespace org.apache.hadoop.io
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
	public abstract class AbstractMapWritable : org.apache.hadoop.io.Writable, org.apache.hadoop.conf.Configurable
	{
		private java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.conf.Configuration
			> conf;

		[com.google.common.annotations.VisibleForTesting]
		internal System.Collections.Generic.IDictionary<java.lang.Class, byte> classToIdMap
			 = new java.util.concurrent.ConcurrentHashMap<java.lang.Class, byte>();

		[com.google.common.annotations.VisibleForTesting]
		internal System.Collections.Generic.IDictionary<byte, java.lang.Class> idToClassMap
			 = new java.util.concurrent.ConcurrentHashMap<byte, java.lang.Class>();

		private volatile byte newClasses = 0;

		/* Class to id mappings */
		/* Id to Class mappings */
		/* The number of new classes (those not established by the constructor) */
		/// <returns>the number of known classes</returns>
		internal virtual byte getNewClasses()
		{
			return newClasses;
		}

		/// <summary>Used to add "predefined" classes and by Writable to copy "new" classes.</summary>
		private void addToMap(java.lang.Class clazz, byte id)
		{
			lock (this)
			{
				if (classToIdMap.Contains(clazz))
				{
					byte b = classToIdMap[clazz];
					if (b != id)
					{
						throw new System.ArgumentException("Class " + clazz.getName() + " already registered but maps to "
							 + b + " and not " + id);
					}
				}
				if (idToClassMap.Contains(id))
				{
					java.lang.Class c = idToClassMap[id];
					if (!c.Equals(clazz))
					{
						throw new System.ArgumentException("Id " + id + " exists but maps to " + c.getName
							() + " and not " + clazz.getName());
					}
				}
				classToIdMap[clazz] = id;
				idToClassMap[id] = clazz;
			}
		}

		/// <summary>Add a Class to the maps if it is not already present.</summary>
		protected internal virtual void addToMap(java.lang.Class clazz)
		{
			lock (this)
			{
				if (classToIdMap.Contains(clazz))
				{
					return;
				}
				if (newClasses + 1 > byte.MaxValue)
				{
					throw new System.IndexOutOfRangeException("adding an additional class would" + " exceed the maximum number allowed"
						);
				}
				byte id = ++newClasses;
				addToMap(clazz, id);
			}
		}

		/// <returns>the Class class for the specified id</returns>
		protected internal virtual java.lang.Class getClass(byte id)
		{
			return idToClassMap[id];
		}

		/// <returns>the id for the specified Class</returns>
		protected internal virtual byte getId(java.lang.Class clazz)
		{
			return classToIdMap.Contains(clazz) ? classToIdMap[clazz] : -1;
		}

		/// <summary>Used by child copy constructors.</summary>
		protected internal virtual void copy(org.apache.hadoop.io.Writable other)
		{
			lock (this)
			{
				if (other != null)
				{
					try
					{
						org.apache.hadoop.io.DataOutputBuffer @out = new org.apache.hadoop.io.DataOutputBuffer
							();
						other.write(@out);
						org.apache.hadoop.io.DataInputBuffer @in = new org.apache.hadoop.io.DataInputBuffer
							();
						@in.reset(@out.getData(), @out.getLength());
						readFields(@in);
					}
					catch (System.IO.IOException e)
					{
						throw new System.ArgumentException("map cannot be copied: " + e.Message);
					}
				}
				else
				{
					throw new System.ArgumentException("source map cannot be null");
				}
			}
		}

		/// <summary>constructor.</summary>
		protected internal AbstractMapWritable()
		{
			this.conf = new java.util.concurrent.atomic.AtomicReference<org.apache.hadoop.conf.Configuration
				>();
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ArrayWritable
				)), byte.valueOf(int.Parse(-127)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BooleanWritable
				)), byte.valueOf(int.Parse(-126)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.BytesWritable
				)), byte.valueOf(int.Parse(-125)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.FloatWritable
				)), byte.valueOf(int.Parse(-124)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.IntWritable)
				), byte.valueOf(int.Parse(-123)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.LongWritable
				)), byte.valueOf(int.Parse(-122)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.MapWritable)
				), byte.valueOf(int.Parse(-121)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.MD5Hash)), byte
				.valueOf(int.Parse(-120)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.NullWritable
				)), byte.valueOf(int.Parse(-119)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.ObjectWritable
				)), byte.valueOf(int.Parse(-118)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.SortedMapWritable
				)), byte.valueOf(int.Parse(-117)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.Text)), byte
				.valueOf(int.Parse(-116)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.TwoDArrayWritable
				)), byte.valueOf(int.Parse(-115)));
			// UTF8 is deprecated so we don't support it
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.VIntWritable
				)), byte.valueOf(int.Parse(-114)));
			addToMap(Sharpen.Runtime.getClassForType(typeof(org.apache.hadoop.io.VLongWritable
				)), byte.valueOf(int.Parse(-113)));
		}

		/// <returns>the conf</returns>
		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return conf.get();
		}

		/// <param name="conf">the conf to set</param>
		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf.set(conf);
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			// First write out the size of the class table and any classes that are
			// "unknown" classes
			@out.writeByte(newClasses);
			for (byte i = 1; ((sbyte)i) <= newClasses; i++)
			{
				@out.writeByte(i);
				@out.writeUTF(getClass(i).getName());
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			// Get the number of "unknown" classes
			newClasses = @in.readByte();
			// Then read in the class names and add them to our tables
			for (int i = 0; i < newClasses; i++)
			{
				byte id = @in.readByte();
				string className = @in.readUTF();
				try
				{
					addToMap(java.lang.Class.forName(className), id);
				}
				catch (java.lang.ClassNotFoundException e)
				{
					throw new System.IO.IOException("can't find class: " + className + " because " + 
						e.Message);
				}
			}
		}
	}
}
