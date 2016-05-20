using Sharpen;

namespace org.apache.hadoop.io
{
	/// <summary>A Writable wrapper for EnumSet.</summary>
	public class EnumSetWritable<E> : java.util.AbstractCollection<E>, org.apache.hadoop.io.Writable
		, org.apache.hadoop.conf.Configurable
		where E : java.lang.Enum<E>
	{
		private java.util.EnumSet<E> value;

		[System.NonSerialized]
		private java.lang.Class elementType;

		[System.NonSerialized]
		private org.apache.hadoop.conf.Configuration conf;

		internal EnumSetWritable()
		{
		}

		public override System.Collections.Generic.IEnumerator<E> GetEnumerator()
		{
			return value.GetEnumerator();
		}

		public override int Count
		{
			get
			{
				return value.Count;
			}
		}

		public override bool add(E e)
		{
			if (value == null)
			{
				value = java.util.EnumSet.of(e);
				set(value, null);
			}
			return value.add(e);
		}

		/// <summary>Construct a new EnumSetWritable.</summary>
		/// <remarks>
		/// Construct a new EnumSetWritable. If the <tt>value</tt> argument is null or
		/// its size is zero, the <tt>elementType</tt> argument must not be null. If
		/// the argument <tt>value</tt>'s size is bigger than zero, the argument
		/// <tt>elementType</tt> is not be used.
		/// </remarks>
		/// <param name="value"/>
		/// <param name="elementType"/>
		public EnumSetWritable(java.util.EnumSet<E> value, java.lang.Class elementType)
		{
			set(value, elementType);
		}

		/// <summary>Construct a new EnumSetWritable.</summary>
		/// <remarks>
		/// Construct a new EnumSetWritable. Argument <tt>value</tt> should not be null
		/// or empty.
		/// </remarks>
		/// <param name="value"/>
		public EnumSetWritable(java.util.EnumSet<E> value)
			: this(value, null)
		{
		}

		/// <summary>
		/// reset the EnumSetWritable with specified
		/// <tt>value</value> and <tt>elementType</tt>.
		/// </summary>
		/// <remarks>
		/// reset the EnumSetWritable with specified
		/// <tt>value</value> and <tt>elementType</tt>. If the <tt>value</tt> argument
		/// is null or its size is zero, the <tt>elementType</tt> argument must not be
		/// null. If the argument <tt>value</tt>'s size is bigger than zero, the
		/// argument <tt>elementType</tt> is not be used.
		/// </remarks>
		/// <param name="value"/>
		/// <param name="elementType"/>
		public virtual void set(java.util.EnumSet<E> value, java.lang.Class elementType)
		{
			if ((value == null || value.Count == 0) && (this.elementType == null && elementType
				 == null))
			{
				throw new System.ArgumentException("The EnumSet argument is null, or is an empty set but with no elementType provided."
					);
			}
			this.value = value;
			if (value != null && value.Count > 0)
			{
				System.Collections.Generic.IEnumerator<E> iterator = value.GetEnumerator();
				this.elementType = iterator.Current.getDeclaringClass();
			}
			else
			{
				if (elementType != null)
				{
					this.elementType = elementType;
				}
			}
		}

		/// <summary>Return the value of this EnumSetWritable.</summary>
		public virtual java.util.EnumSet<E> get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void readFields(java.io.DataInput @in)
		{
			int length = @in.readInt();
			if (length == -1)
			{
				this.value = null;
			}
			else
			{
				if (length == 0)
				{
					this.elementType = (java.lang.Class)org.apache.hadoop.io.ObjectWritable.loadClass
						(conf, org.apache.hadoop.io.WritableUtils.readString(@in));
					this.value = java.util.EnumSet.noneOf(this.elementType);
				}
				else
				{
					E first = (E)org.apache.hadoop.io.ObjectWritable.readObject(@in, conf);
					this.value = (java.util.EnumSet<E>)java.util.EnumSet.of(first);
					for (int i = 1; i < length; i++)
					{
						this.value.add((E)org.apache.hadoop.io.ObjectWritable.readObject(@in, conf));
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void write(java.io.DataOutput @out)
		{
			if (this.value == null)
			{
				@out.writeInt(-1);
				org.apache.hadoop.io.WritableUtils.writeString(@out, this.elementType.getName());
			}
			else
			{
				object[] array = Sharpen.Collections.ToArray(this.value);
				int length = array.Length;
				@out.writeInt(length);
				if (length == 0)
				{
					if (this.elementType == null)
					{
						throw new System.NotSupportedException("Unable to serialize empty EnumSet with no element type provided."
							);
					}
					org.apache.hadoop.io.WritableUtils.writeString(@out, this.elementType.getName());
				}
				for (int i = 0; i < length; i++)
				{
					org.apache.hadoop.io.ObjectWritable.writeObject(@out, array[i], Sharpen.Runtime.getClassForObject
						(array[i]), conf);
				}
			}
		}

		/// <summary>
		/// Returns true if <code>o</code> is an EnumSetWritable with the same value,
		/// or both are null.
		/// </summary>
		public override bool Equals(object o)
		{
			if (o == null)
			{
				throw new System.ArgumentException("null argument passed in equal().");
			}
			if (!(o is org.apache.hadoop.io.EnumSetWritable))
			{
				return false;
			}
			org.apache.hadoop.io.EnumSetWritable<object> other = (org.apache.hadoop.io.EnumSetWritable
				<object>)o;
			if (this == o || (this.value == other.value))
			{
				return true;
			}
			if (this.value == null)
			{
				// other.value must not be null if we reach here
				return false;
			}
			return this.value.Equals(other.value);
		}

		/// <summary>Returns the class of all the elements of the underlying EnumSetWriable.</summary>
		/// <remarks>
		/// Returns the class of all the elements of the underlying EnumSetWriable. It
		/// may return null.
		/// </remarks>
		/// <returns>the element class</returns>
		public virtual java.lang.Class getElementType()
		{
			return elementType;
		}

		public override int GetHashCode()
		{
			if (value == null)
			{
				return 0;
			}
			return (int)value.GetHashCode();
		}

		public override string ToString()
		{
			if (value == null)
			{
				return "(null)";
			}
			return value.ToString();
		}

		public virtual org.apache.hadoop.conf.Configuration getConf()
		{
			return this.conf;
		}

		public virtual void setConf(org.apache.hadoop.conf.Configuration conf)
		{
			this.conf = conf;
		}

		static EnumSetWritable()
		{
			org.apache.hadoop.io.WritableFactories.setFactory(Sharpen.Runtime.getClassForType
				(typeof(org.apache.hadoop.io.EnumSetWritable)), new _WritableFactory_211());
		}

		private sealed class _WritableFactory_211 : org.apache.hadoop.io.WritableFactory
		{
			public _WritableFactory_211()
			{
			}

			public org.apache.hadoop.io.Writable newInstance()
			{
				return new org.apache.hadoop.io.EnumSetWritable();
			}
		}
	}
}
