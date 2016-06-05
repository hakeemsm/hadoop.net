using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>
	/// <p>
	/// This class contains the information about a timeline domain, which is used
	/// to a user to host a number of timeline entities, isolating them from others'.
	/// </summary>
	/// <remarks>
	/// <p>
	/// This class contains the information about a timeline domain, which is used
	/// to a user to host a number of timeline entities, isolating them from others'.
	/// The user can also define the reader and writer users/groups for the the
	/// domain, which is used to control the access to its entities.
	/// </p>
	/// <p>
	/// The reader and writer users/groups pattern that the user can supply is the
	/// same as what <code>AccessControlList</code> takes.
	/// </p>
	/// </remarks>
	public class TimelineDomain
	{
		private string id;

		private string description;

		private string owner;

		private string readers;

		private string writers;

		private long createdTime;

		private long modifiedTime;

		public TimelineDomain()
		{
		}

		/// <summary>Get the domain ID</summary>
		/// <returns>the domain ID</returns>
		public virtual string GetId()
		{
			return id;
		}

		/// <summary>Set the domain ID</summary>
		/// <param name="id">the domain ID</param>
		public virtual void SetId(string id)
		{
			this.id = id;
		}

		/// <summary>Get the domain description</summary>
		/// <returns>the domain description</returns>
		public virtual string GetDescription()
		{
			return description;
		}

		/// <summary>Set the domain description</summary>
		/// <param name="description">the domain description</param>
		public virtual void SetDescription(string description)
		{
			this.description = description;
		}

		/// <summary>Get the domain owner</summary>
		/// <returns>the domain owner</returns>
		public virtual string GetOwner()
		{
			return owner;
		}

		/// <summary>Set the domain owner.</summary>
		/// <remarks>
		/// Set the domain owner. The user doesn't need to set it, which will
		/// automatically set to the user who puts the domain.
		/// </remarks>
		/// <param name="owner">the domain owner</param>
		public virtual void SetOwner(string owner)
		{
			this.owner = owner;
		}

		/// <summary>Get the reader (and/or reader group) list string</summary>
		/// <returns>the reader (and/or reader group) list string</returns>
		public virtual string GetReaders()
		{
			return readers;
		}

		/// <summary>Set the reader (and/or reader group) list string</summary>
		/// <param name="readers">the reader (and/or reader group) list string</param>
		public virtual void SetReaders(string readers)
		{
			this.readers = readers;
		}

		/// <summary>Get the writer (and/or writer group) list string</summary>
		/// <returns>the writer (and/or writer group) list string</returns>
		public virtual string GetWriters()
		{
			return writers;
		}

		/// <summary>Set the writer (and/or writer group) list string</summary>
		/// <param name="writers">the writer (and/or writer group) list string</param>
		public virtual void SetWriters(string writers)
		{
			this.writers = writers;
		}

		/// <summary>Get the created time of the domain</summary>
		/// <returns>the created time of the domain</returns>
		public virtual long GetCreatedTime()
		{
			return createdTime;
		}

		/// <summary>Set the created time of the domain</summary>
		/// <param name="createdTime">the created time of the domain</param>
		public virtual void SetCreatedTime(long createdTime)
		{
			this.createdTime = createdTime;
		}

		/// <summary>Get the modified time of the domain</summary>
		/// <returns>the modified time of the domain</returns>
		public virtual long GetModifiedTime()
		{
			return modifiedTime;
		}

		/// <summary>Set the modified time of the domain</summary>
		/// <param name="modifiedTime">the modified time of the domain</param>
		public virtual void SetModifiedTime(long modifiedTime)
		{
			this.modifiedTime = modifiedTime;
		}
	}
}
