using System;
using System.Collections.Generic;
using System.IO;
using Org.Iq80.Leveldb;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Utils
{
	/// <summary>
	/// A wrapper for a DBIterator to translate the raw RuntimeExceptions that
	/// can be thrown into DBExceptions.
	/// </summary>
	public class LeveldbIterator : IEnumerator<KeyValuePair<byte[], byte[]>>, IDisposable
	{
		private DBIterator iter;

		/// <summary>Create an iterator for the specified database</summary>
		public LeveldbIterator(DB db)
		{
			iter = db.GetEnumerator();
		}

		/// <summary>Create an iterator for the specified database</summary>
		public LeveldbIterator(DB db, ReadOptions options)
		{
			iter = db.Iterator(options);
		}

		/// <summary>Create an iterator using the specified underlying DBIterator</summary>
		public LeveldbIterator(DBIterator iter)
		{
			this.iter = iter;
		}

		/// <summary>
		/// Repositions the iterator so the key of the next BlockElement
		/// returned greater than or equal to the specified targetKey.
		/// </summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual void Seek(byte[] key)
		{
			try
			{
				iter.Seek(key);
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Repositions the iterator so is is at the beginning of the Database.</summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual void SeekToFirst()
		{
			try
			{
				iter.SeekToFirst();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Repositions the iterator so it is at the end of of the Database.</summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual void SeekToLast()
		{
			try
			{
				iter.SeekToLast();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Returns <tt>true</tt> if the iteration has more elements.</summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual bool HasNext()
		{
			try
			{
				return iter.HasNext();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Returns the next element in the iteration.</summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual KeyValuePair<byte[], byte[]> Next()
		{
			try
			{
				return iter.Next();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>
		/// Returns the next element in the iteration, without advancing the
		/// iteration.
		/// </summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual KeyValuePair<byte[], byte[]> PeekNext()
		{
			try
			{
				return iter.PeekNext();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <returns>true if there is a previous entry in the iteration.</returns>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual bool HasPrev()
		{
			try
			{
				return iter.HasPrev();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <returns>the previous element in the iteration and rewinds the iteration.</returns>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual KeyValuePair<byte[], byte[]> Prev()
		{
			try
			{
				return iter.Prev();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <returns>
		/// the previous element in the iteration, without rewinding the
		/// iteration.
		/// </returns>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual KeyValuePair<byte[], byte[]> PeekPrev()
		{
			try
			{
				return iter.PeekPrev();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Removes from the database the last element returned by the iterator.</summary>
		/// <exception cref="Org.Iq80.Leveldb.DBException"/>
		public virtual void Remove()
		{
			try
			{
				iter.Remove();
			}
			catch (DBException e)
			{
				throw;
			}
			catch (RuntimeException e)
			{
				throw new DBException(e.Message, e);
			}
		}

		/// <summary>Closes the iterator.</summary>
		/// <exception cref="System.IO.IOException"/>
		public virtual void Close()
		{
			try
			{
				iter.Close();
			}
			catch (RuntimeException e)
			{
				throw new IOException(e.Message, e);
			}
		}
	}
}
