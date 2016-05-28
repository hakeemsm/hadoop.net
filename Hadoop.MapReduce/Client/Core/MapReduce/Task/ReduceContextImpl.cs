using System;
using System.Collections.Generic;
using System.IO;
using Org.Apache.Hadoop.Conf;
using Org.Apache.Hadoop.IO;
using Org.Apache.Hadoop.IO.Serializer;
using Org.Apache.Hadoop.Mapred;
using Org.Apache.Hadoop.Mapreduce;
using Org.Apache.Hadoop.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Mapreduce.Task
{
	/// <summary>
	/// The context passed to the
	/// <see cref="Org.Apache.Hadoop.Mapreduce.Reducer{KEYIN, VALUEIN, KEYOUT, VALUEOUT}"
	/// 	/>
	/// .
	/// </summary>
	/// <?/>
	/// <?/>
	/// <?/>
	/// <?/>
	public class ReduceContextImpl<Keyin, Valuein, Keyout, Valueout> : TaskInputOutputContextImpl
		<KEYIN, VALUEIN, KEYOUT, VALUEOUT>, ReduceContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT
		>
	{
		private RawKeyValueIterator input;

		private Counter inputValueCounter;

		private Counter inputKeyCounter;

		private RawComparator<KEYIN> comparator;

		private KEYIN key;

		private VALUEIN value;

		private bool firstValue = false;

		private bool nextKeyIsSame = false;

		private bool hasMore;

		protected internal Progressable reporter;

		private Deserializer<KEYIN> keyDeserializer;

		private Deserializer<VALUEIN> valueDeserializer;

		private DataInputBuffer buffer = new DataInputBuffer();

		private BytesWritable currentRawKey = new BytesWritable();

		private ReduceContextImpl.ValueIterable iterable;

		private bool isMarked = false;

		private BackupStore<KEYIN, VALUEIN> backupStore;

		private readonly SerializationFactory serializationFactory;

		private readonly Type keyClass;

		private readonly Type valueClass;

		private readonly Configuration conf;

		private readonly TaskAttemptID taskid;

		private int currentKeyLength = -1;

		private int currentValueLength = -1;

		/// <exception cref="System.Exception"/>
		/// <exception cref="System.IO.IOException"/>
		public ReduceContextImpl(Configuration conf, TaskAttemptID taskid, RawKeyValueIterator
			 input, Counter inputKeyCounter, Counter inputValueCounter, RecordWriter<KEYOUT, 
			VALUEOUT> output, OutputCommitter committer, StatusReporter reporter, RawComparator
			<KEYIN> comparator, Type keyClass, Type valueClass)
			: base(conf, taskid, output, committer, reporter)
		{
			iterable = new ReduceContextImpl.ValueIterable(this);
			// current key
			// current value
			// first value in key
			// more w/ this key
			// more in file
			this.input = input;
			this.inputKeyCounter = inputKeyCounter;
			this.inputValueCounter = inputValueCounter;
			this.comparator = comparator;
			this.serializationFactory = new SerializationFactory(conf);
			this.keyDeserializer = serializationFactory.GetDeserializer(keyClass);
			this.keyDeserializer.Open(buffer);
			this.valueDeserializer = serializationFactory.GetDeserializer(valueClass);
			this.valueDeserializer.Open(buffer);
			hasMore = input.Next();
			this.keyClass = keyClass;
			this.valueClass = valueClass;
			this.conf = conf;
			this.taskid = taskid;
		}

		/// <summary>Start processing next unique key.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKey()
		{
			while (hasMore && nextKeyIsSame)
			{
				NextKeyValue();
			}
			if (hasMore)
			{
				if (inputKeyCounter != null)
				{
					inputKeyCounter.Increment(1);
				}
				return NextKeyValue();
			}
			else
			{
				return false;
			}
		}

		/// <summary>Advance to the next key/value pair.</summary>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override bool NextKeyValue()
		{
			if (!hasMore)
			{
				key = null;
				value = null;
				return false;
			}
			firstValue = !nextKeyIsSame;
			DataInputBuffer nextKey = input.GetKey();
			currentRawKey.Set(nextKey.GetData(), nextKey.GetPosition(), nextKey.GetLength() -
				 nextKey.GetPosition());
			buffer.Reset(currentRawKey.GetBytes(), 0, currentRawKey.GetLength());
			key = keyDeserializer.Deserialize(key);
			DataInputBuffer nextVal = input.GetValue();
			buffer.Reset(nextVal.GetData(), nextVal.GetPosition(), nextVal.GetLength() - nextVal
				.GetPosition());
			value = valueDeserializer.Deserialize(value);
			currentKeyLength = nextKey.GetLength() - nextKey.GetPosition();
			currentValueLength = nextVal.GetLength() - nextVal.GetPosition();
			if (isMarked)
			{
				backupStore.Write(nextKey, nextVal);
			}
			hasMore = input.Next();
			if (hasMore)
			{
				nextKey = input.GetKey();
				nextKeyIsSame = comparator.Compare(currentRawKey.GetBytes(), 0, currentRawKey.GetLength
					(), nextKey.GetData(), nextKey.GetPosition(), nextKey.GetLength() - nextKey.GetPosition
					()) == 0;
			}
			else
			{
				nextKeyIsSame = false;
			}
			inputValueCounter.Increment(1);
			return true;
		}

		public override KEYIN GetCurrentKey()
		{
			return key;
		}

		public override VALUEIN GetCurrentValue()
		{
			return value;
		}

		internal virtual BackupStore<KEYIN, VALUEIN> GetBackupStore()
		{
			return backupStore;
		}

		protected internal class ValueIterator : ReduceContext.ValueIterator<VALUEIN>
		{
			private bool inReset = false;

			private bool clearMarkFlag = false;

			public virtual bool HasNext()
			{
				try
				{
					if (this.inReset && this._enclosing.backupStore.HasNext())
					{
						return true;
					}
				}
				catch (Exception e)
				{
					Sharpen.Runtime.PrintStackTrace(e);
					throw new RuntimeException("hasNext failed", e);
				}
				return this._enclosing.firstValue || this._enclosing.nextKeyIsSame;
			}

			public virtual VALUEIN Next()
			{
				if (this.inReset)
				{
					try
					{
						if (this._enclosing.backupStore.HasNext())
						{
							this._enclosing.backupStore.Next();
							DataInputBuffer next = this._enclosing.backupStore.NextValue();
							this._enclosing.buffer.Reset(next.GetData(), next.GetPosition(), next.GetLength()
								 - next.GetPosition());
							this._enclosing.value = this._enclosing.valueDeserializer.Deserialize(this._enclosing
								.value);
							return this._enclosing.value;
						}
						else
						{
							this.inReset = false;
							this._enclosing.backupStore.ExitResetMode();
							if (this.clearMarkFlag)
							{
								this.clearMarkFlag = false;
								this._enclosing.isMarked = false;
							}
						}
					}
					catch (IOException e)
					{
						Sharpen.Runtime.PrintStackTrace(e);
						throw new RuntimeException("next value iterator failed", e);
					}
				}
				// if this is the first record, we don't need to advance
				if (this._enclosing.firstValue)
				{
					this._enclosing.firstValue = false;
					return this._enclosing.value;
				}
				// if this isn't the first record and the next key is different, they
				// can't advance it here.
				if (!this._enclosing.nextKeyIsSame)
				{
					throw new NoSuchElementException("iterate past last value");
				}
				// otherwise, go to the next key/value pair
				try
				{
					this._enclosing.NextKeyValue();
					return this._enclosing.value;
				}
				catch (IOException ie)
				{
					throw new RuntimeException("next value iterator failed", ie);
				}
				catch (Exception ie)
				{
					// this is bad, but we can't modify the exception list of java.util
					throw new RuntimeException("next value iterator interrupted", ie);
				}
			}

			public virtual void Remove()
			{
				throw new NotSupportedException("remove not implemented");
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Mark()
			{
				if (this._enclosing.GetBackupStore() == null)
				{
					this._enclosing.backupStore = new BackupStore<KEYIN, VALUEIN>(this._enclosing.conf
						, this._enclosing.taskid);
				}
				this._enclosing.isMarked = true;
				if (!this.inReset)
				{
					this._enclosing.backupStore.Reinitialize();
					if (this._enclosing.currentKeyLength == -1)
					{
						// The user has not called next() for this iterator yet, so
						// there is no current record to mark and copy to backup store.
						return;
					}
					System.Diagnostics.Debug.Assert((this._enclosing.currentValueLength != -1));
					int requestedSize = this._enclosing.currentKeyLength + this._enclosing.currentValueLength
						 + WritableUtils.GetVIntSize(this._enclosing.currentKeyLength) + WritableUtils.GetVIntSize
						(this._enclosing.currentValueLength);
					DataOutputStream @out = this._enclosing.backupStore.GetOutputStream(requestedSize
						);
					this.WriteFirstKeyValueBytes(@out);
					this._enclosing.backupStore.UpdateCounters(requestedSize);
				}
				else
				{
					this._enclosing.backupStore.Mark();
				}
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void Reset()
			{
				// We reached the end of an iteration and user calls a 
				// reset, but a clearMark was called before, just throw
				// an exception
				if (this.clearMarkFlag)
				{
					this.clearMarkFlag = false;
					this._enclosing.backupStore.ClearMark();
					throw new IOException("Reset called without a previous mark");
				}
				if (!this._enclosing.isMarked)
				{
					throw new IOException("Reset called without a previous mark");
				}
				this.inReset = true;
				this._enclosing.backupStore.Reset();
			}

			/// <exception cref="System.IO.IOException"/>
			public virtual void ClearMark()
			{
				if (this._enclosing.GetBackupStore() == null)
				{
					return;
				}
				if (this.inReset)
				{
					this.clearMarkFlag = true;
					this._enclosing.backupStore.ClearMark();
				}
				else
				{
					this.inReset = this._enclosing.isMarked = false;
					this._enclosing.backupStore.Reinitialize();
				}
			}

			/// <summary>
			/// This method is called when the reducer moves from one key to
			/// another.
			/// </summary>
			/// <exception cref="System.IO.IOException"/>
			public virtual void ResetBackupStore()
			{
				if (this._enclosing.GetBackupStore() == null)
				{
					return;
				}
				this.inReset = this._enclosing.isMarked = false;
				this._enclosing.backupStore.Reinitialize();
				this._enclosing.currentKeyLength = -1;
			}

			/// <summary>
			/// This method is called to write the record that was most recently
			/// served (before a call to the mark).
			/// </summary>
			/// <remarks>
			/// This method is called to write the record that was most recently
			/// served (before a call to the mark). Since the framework reads one
			/// record in advance, to get this record, we serialize the current key
			/// and value
			/// </remarks>
			/// <param name="out"/>
			/// <exception cref="System.IO.IOException"/>
			private void WriteFirstKeyValueBytes(DataOutputStream @out)
			{
				System.Diagnostics.Debug.Assert((this._enclosing.GetCurrentKey() != null && this.
					_enclosing.GetCurrentValue() != null));
				WritableUtils.WriteVInt(@out, this._enclosing.currentKeyLength);
				WritableUtils.WriteVInt(@out, this._enclosing.currentValueLength);
				Org.Apache.Hadoop.IO.Serializer.Serializer<KEYIN> keySerializer = this._enclosing
					.serializationFactory.GetSerializer(this._enclosing.keyClass);
				keySerializer.Open(@out);
				keySerializer.Serialize(this._enclosing.GetCurrentKey());
				Org.Apache.Hadoop.IO.Serializer.Serializer<VALUEIN> valueSerializer = this._enclosing
					.serializationFactory.GetSerializer(this._enclosing.valueClass);
				valueSerializer.Open(@out);
				valueSerializer.Serialize(this._enclosing.GetCurrentValue());
			}

			internal ValueIterator(ReduceContextImpl<Keyin, Valuein, Keyout, Valueout> _enclosing
				)
			{
				this._enclosing = _enclosing;
			}

			private readonly ReduceContextImpl<Keyin, Valuein, Keyout, Valueout> _enclosing;
		}

		protected internal class ValueIterable : IEnumerable<VALUEIN>
		{
			private ReduceContextImpl.ValueIterator iterator;

			public override IEnumerator<VALUEIN> GetEnumerator()
			{
				return this.iterator;
			}

			public ValueIterable(ReduceContextImpl<Keyin, Valuein, Keyout, Valueout> _enclosing
				)
			{
				this._enclosing = _enclosing;
				iterator = new ReduceContextImpl.ValueIterator(this);
			}

			private readonly ReduceContextImpl<Keyin, Valuein, Keyout, Valueout> _enclosing;
		}

		/// <summary>
		/// Iterate through the values for the current key, reusing the same value
		/// object, which is stored in the context.
		/// </summary>
		/// <returns>
		/// the series of values associated with the current key. All of the
		/// objects returned directly and indirectly from this method are reused.
		/// </returns>
		/// <exception cref="System.IO.IOException"/>
		/// <exception cref="System.Exception"/>
		public override IEnumerable<VALUEIN> GetValues()
		{
			return iterable;
		}
	}
}
