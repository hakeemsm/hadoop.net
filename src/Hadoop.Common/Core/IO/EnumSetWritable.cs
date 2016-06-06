using System;
using System.Collections.Generic;
using System.IO;
using Hadoop.Common.Core.Conf;
using Org.Apache.Hadoop.IO;

namespace Hadoop.Common.Core.IO
{
	/// <summary>A Writable wrapper for EnumSet.</summary>
	public class EnumSetWritable<E> : AbstractCollection<E>, IWritable, Configurable
	{
		private EnumSet<E> value;

		[System.NonSerialized]
		private Type _elementType;

		[System.NonSerialized]
		private Configuration _conf;

		internal EnumSetWritable()
		{
		}

		public override IEnumerator<E> GetEnumerator()
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

		public override bool AddItem(E e)
		{
			if (value == null)
			{
				value = EnumSet.Of(e);
				Set(value, null);
			}
			return value.AddItem(e);
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
		public EnumSetWritable(EnumSet<E> value, Type elementType)
		{
			Set(value, elementType);
		}

		/// <summary>Construct a new EnumSetWritable.</summary>
		/// <remarks>
		/// Construct a new EnumSetWritable. Argument <tt>value</tt> should not be null
		/// or empty.
		/// </remarks>
		/// <param name="value"/>
		public EnumSetWritable(EnumSet<E> value)
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
		public virtual void Set(EnumSet<E> value, Type elementType)
		{
			if ((value == null || value.Count == 0) && (this._elementType == null && elementType
				 == null))
			{
				throw new ArgumentException("The EnumSet argument is null, or is an empty set but with no elementType provided."
					);
			}
			this.value = value;
			if (value != null && value.Count > 0)
			{
				IEnumerator<E> iterator = value.GetEnumerator();
				this._elementType = iterator.Next().GetDeclaringClass();
			}
			else
			{
				if (elementType != null)
				{
					this._elementType = elementType;
				}
			}
		}

		/// <summary>Return the value of this EnumSetWritable.</summary>
		public virtual EnumSet<E> Get()
		{
			return value;
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void ReadFields(BinaryReader reader)
		{
			int length = @in.ReadInt();
			if (length == -1)
			{
				this.value = null;
			}
			else
			{
				if (length == 0)
				{
					this._elementType = (Type)ObjectWritable.LoadClass(_conf, WritableUtils.ReadString(
						@in));
					this.value = EnumSet.NoneOf(this._elementType);
				}
				else
				{
					E first = (E)ObjectWritable.ReadObject(@in, _conf);
					this.value = (EnumSet<E>)EnumSet.Of(first);
					for (int i = 1; i < length; i++)
					{
						this.value.AddItem((E)ObjectWritable.ReadObject(@in, _conf));
					}
				}
			}
		}

		/// <exception cref="System.IO.IOException"/>
		public virtual void Write(BinaryWriter writer)
		{
			if (this.value == null)
			{
				@out.WriteInt(-1);
				WritableUtils.WriteString(@out, this._elementType.FullName);
			}
			else
			{
				object[] array = Collections.ToArray(this.value);
				int length = array.Length;
				@out.WriteInt(length);
				if (length == 0)
				{
					if (this._elementType == null)
					{
						throw new NotSupportedException("Unable to serialize empty EnumSet with no element type provided."
							);
					}
					WritableUtils.WriteString(@out, this._elementType.FullName);
				}
				for (int i = 0; i < length; i++)
				{
					ObjectWritable.WriteObject(@out, array[i], array[i].GetType(), _conf);
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
				throw new ArgumentException("null argument passed in equal().");
			}
			if (!(o is Org.Apache.Hadoop.IO.EnumSetWritable))
			{
				return false;
			}
			EnumSetWritable<object> other = (EnumSetWritable
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
		public virtual Type GetElementType()
		{
			return _elementType;
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

		public virtual Configuration GetConf()
		{
			return this._conf;
		}

		public virtual void SetConf(Configuration conf)
		{
			this._conf = conf;
		}

		static EnumSetWritable()
		{
			WritableFactories.SetFactory(typeof(Org.Apache.Hadoop.IO.EnumSetWritable), new _WritableFactory_211
				());
		}

		private sealed class _WritableFactory_211 : WritableFactory
		{
			public _WritableFactory_211()
			{
			}

			public IWritable NewInstance()
			{
				return new Org.Apache.Hadoop.IO.EnumSetWritable();
			}
		}
	}
}
