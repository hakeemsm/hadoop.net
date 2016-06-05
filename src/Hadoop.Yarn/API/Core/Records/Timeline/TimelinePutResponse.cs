using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>A class that holds a list of put errors.</summary>
	/// <remarks>
	/// A class that holds a list of put errors. This is the response returned when a
	/// list of
	/// <see cref="TimelineEntity"/>
	/// objects is added to the timeline. If there are errors
	/// in storing individual entity objects, they will be indicated in the list of
	/// errors.
	/// </remarks>
	public class TimelinePutResponse
	{
		private IList<TimelinePutResponse.TimelinePutError> errors = new AList<TimelinePutResponse.TimelinePutError
			>();

		public TimelinePutResponse()
		{
		}

		/// <summary>
		/// Get a list of
		/// <see cref="TimelinePutError"/>
		/// instances
		/// </summary>
		/// <returns>
		/// a list of
		/// <see cref="TimelinePutError"/>
		/// instances
		/// </returns>
		public virtual IList<TimelinePutResponse.TimelinePutError> GetErrors()
		{
			return errors;
		}

		/// <summary>
		/// Add a single
		/// <see cref="TimelinePutError"/>
		/// instance into the existing list
		/// </summary>
		/// <param name="error">
		/// a single
		/// <see cref="TimelinePutError"/>
		/// instance
		/// </param>
		public virtual void AddError(TimelinePutResponse.TimelinePutError error)
		{
			errors.AddItem(error);
		}

		/// <summary>
		/// Add a list of
		/// <see cref="TimelinePutError"/>
		/// instances into the existing list
		/// </summary>
		/// <param name="errors">
		/// a list of
		/// <see cref="TimelinePutError"/>
		/// instances
		/// </param>
		public virtual void AddErrors(IList<TimelinePutResponse.TimelinePutError> errors)
		{
			Sharpen.Collections.AddAll(this.errors, errors);
		}

		/// <summary>
		/// Set the list to the given list of
		/// <see cref="TimelinePutError"/>
		/// instances
		/// </summary>
		/// <param name="errors">
		/// a list of
		/// <see cref="TimelinePutError"/>
		/// instances
		/// </param>
		public virtual void SetErrors(IList<TimelinePutResponse.TimelinePutError> errors)
		{
			this.errors.Clear();
			Sharpen.Collections.AddAll(this.errors, errors);
		}

		/// <summary>A class that holds the error code for one entity.</summary>
		public class TimelinePutError
		{
			/// <summary>
			/// Error code returned when no start time can be found when putting an
			/// entity.
			/// </summary>
			/// <remarks>
			/// Error code returned when no start time can be found when putting an
			/// entity. This occurs when the entity does not already exist in the store
			/// and it is put with no start time or events specified.
			/// </remarks>
			public const int NoStartTime = 1;

			/// <summary>
			/// Error code returned if an IOException is encountered when putting an
			/// entity.
			/// </summary>
			public const int IoException = 2;

			/// <summary>
			/// Error code returned if the user specifies the timeline system reserved
			/// filter key
			/// </summary>
			public const int SystemFilterConflict = 3;

			/// <summary>Error code returned if the user is denied to access the timeline data</summary>
			public const int AccessDenied = 4;

			/// <summary>Error code returned if the entity doesn't have an valid domain ID</summary>
			public const int NoDomain = 5;

			/// <summary>
			/// Error code returned if the user is denied to relate the entity to another
			/// one in different domain
			/// </summary>
			public const int ForbiddenRelation = 6;

			private string entityId;

			private string entityType;

			private int errorCode;

			/// <summary>Get the entity Id</summary>
			/// <returns>the entity Id</returns>
			public virtual string GetEntityId()
			{
				return entityId;
			}

			/// <summary>Set the entity Id</summary>
			/// <param name="entityId">the entity Id</param>
			public virtual void SetEntityId(string entityId)
			{
				this.entityId = entityId;
			}

			/// <summary>Get the entity type</summary>
			/// <returns>the entity type</returns>
			public virtual string GetEntityType()
			{
				return entityType;
			}

			/// <summary>Set the entity type</summary>
			/// <param name="entityType">the entity type</param>
			public virtual void SetEntityType(string entityType)
			{
				this.entityType = entityType;
			}

			/// <summary>Get the error code</summary>
			/// <returns>an error code</returns>
			public virtual int GetErrorCode()
			{
				return errorCode;
			}

			/// <summary>Set the error code to the given error code</summary>
			/// <param name="errorCode">an error code</param>
			public virtual void SetErrorCode(int errorCode)
			{
				this.errorCode = errorCode;
			}
		}
	}
}
