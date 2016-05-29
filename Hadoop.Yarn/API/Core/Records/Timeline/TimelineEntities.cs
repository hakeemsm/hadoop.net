using System.Collections.Generic;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records.Timeline
{
	/// <summary>The class that hosts a list of timeline entities.</summary>
	public class TimelineEntities
	{
		private IList<TimelineEntity> entities = new AList<TimelineEntity>();

		public TimelineEntities()
		{
		}

		/// <summary>Get a list of entities</summary>
		/// <returns>a list of entities</returns>
		public virtual IList<TimelineEntity> GetEntities()
		{
			return entities;
		}

		/// <summary>Add a single entity into the existing entity list</summary>
		/// <param name="entity">a single entity</param>
		public virtual void AddEntity(TimelineEntity entity)
		{
			entities.AddItem(entity);
		}

		/// <summary>All a list of entities into the existing entity list</summary>
		/// <param name="entities">a list of entities</param>
		public virtual void AddEntities(IList<TimelineEntity> entities)
		{
			Sharpen.Collections.AddAll(this.entities, entities);
		}

		/// <summary>Set the entity list to the given list of entities</summary>
		/// <param name="entities">a list of entities</param>
		public virtual void SetEntities(IList<TimelineEntity> entities)
		{
			this.entities = entities;
		}
	}
}
