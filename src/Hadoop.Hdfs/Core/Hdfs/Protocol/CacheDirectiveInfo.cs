using System;
using System.Text;
using Com.Google.Common.Base;
using Org.Apache.Commons.Lang.Builder;
using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Hdfs;
using Sharpen;

namespace Org.Apache.Hadoop.Hdfs.Protocol
{
	/// <summary>Describes a path-based cache directive.</summary>
	public class CacheDirectiveInfo
	{
		/// <summary>A builder for creating new CacheDirectiveInfo instances.</summary>
		public class Builder
		{
			private long id;

			private Path path;

			private short replication;

			private string pool;

			private CacheDirectiveInfo.Expiration expiration;

			/// <summary>Builds a new CacheDirectiveInfo populated with the set properties.</summary>
			/// <returns>New CacheDirectiveInfo.</returns>
			public virtual CacheDirectiveInfo Build()
			{
				return new CacheDirectiveInfo(id, path, replication, pool, expiration);
			}

			/// <summary>Creates an empty builder.</summary>
			public Builder()
			{
			}

			/// <summary>
			/// Creates a builder with all elements set to the same values as the
			/// given CacheDirectiveInfo.
			/// </summary>
			public Builder(CacheDirectiveInfo directive)
			{
				this.id = directive.GetId();
				this.path = directive.GetPath();
				this.replication = directive.GetReplication();
				this.pool = directive.GetPool();
				this.expiration = directive.GetExpiration();
			}

			/// <summary>Sets the id used in this request.</summary>
			/// <param name="id">The id used in this request.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveInfo.Builder SetId(long id)
			{
				this.id = id;
				return this;
			}

			/// <summary>Sets the path used in this request.</summary>
			/// <param name="path">The path used in this request.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveInfo.Builder SetPath(Path path)
			{
				this.path = path;
				return this;
			}

			/// <summary>Sets the replication used in this request.</summary>
			/// <param name="replication">The replication used in this request.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveInfo.Builder SetReplication(short replication)
			{
				this.replication = replication;
				return this;
			}

			/// <summary>Sets the pool used in this request.</summary>
			/// <param name="pool">The pool used in this request.</param>
			/// <returns>This builder, for call chaining.</returns>
			public virtual CacheDirectiveInfo.Builder SetPool(string pool)
			{
				this.pool = pool;
				return this;
			}

			/// <summary>Sets when the CacheDirective should expire.</summary>
			/// <remarks>
			/// Sets when the CacheDirective should expire. A
			/// <see cref="Expiration"/>
			/// can specify either an absolute or
			/// relative expiration time.
			/// </remarks>
			/// <param name="expiration">when this CacheDirective should expire</param>
			/// <returns>This builder, for call chaining</returns>
			public virtual CacheDirectiveInfo.Builder SetExpiration(CacheDirectiveInfo.Expiration
				 expiration)
			{
				this.expiration = expiration;
				return this;
			}
		}

		/// <summary>Denotes a relative or absolute expiration time for a CacheDirective.</summary>
		/// <remarks>
		/// Denotes a relative or absolute expiration time for a CacheDirective. Use
		/// factory methods
		/// <see cref="NewAbsolute(System.DateTime)"/>
		/// and
		/// <see cref="NewRelative(long)"/>
		/// to create an
		/// Expiration.
		/// <p>
		/// In either case, the server-side clock is used to determine when a
		/// CacheDirective expires.
		/// </remarks>
		public class Expiration
		{
			/// <summary>The maximum value we accept for a relative expiry.</summary>
			public const long MaxRelativeExpiryMs = long.MaxValue / 4;

			/// <summary>An relative Expiration that never expires.</summary>
			public static readonly CacheDirectiveInfo.Expiration Never = NewRelative(MaxRelativeExpiryMs
				);

			// This helps prevent weird overflow bugs
			/// <summary>Create a new relative Expiration.</summary>
			/// <remarks>
			/// Create a new relative Expiration.
			/// <p>
			/// Use
			/// <see cref="Never"/>
			/// to indicate an Expiration that never
			/// expires.
			/// </remarks>
			/// <param name="ms">how long until the CacheDirective expires, in milliseconds</param>
			/// <returns>A relative Expiration</returns>
			public static CacheDirectiveInfo.Expiration NewRelative(long ms)
			{
				return new CacheDirectiveInfo.Expiration(ms, true);
			}

			/// <summary>Create a new absolute Expiration.</summary>
			/// <remarks>
			/// Create a new absolute Expiration.
			/// <p>
			/// Use
			/// <see cref="Never"/>
			/// to indicate an Expiration that never
			/// expires.
			/// </remarks>
			/// <param name="date">when the CacheDirective expires</param>
			/// <returns>An absolute Expiration</returns>
			public static CacheDirectiveInfo.Expiration NewAbsolute(DateTime date)
			{
				return new CacheDirectiveInfo.Expiration(date.GetTime(), false);
			}

			/// <summary>Create a new absolute Expiration.</summary>
			/// <remarks>
			/// Create a new absolute Expiration.
			/// <p>
			/// Use
			/// <see cref="Never"/>
			/// to indicate an Expiration that never
			/// expires.
			/// </remarks>
			/// <param name="ms">
			/// when the CacheDirective expires, in milliseconds since the Unix
			/// epoch.
			/// </param>
			/// <returns>An absolute Expiration</returns>
			public static CacheDirectiveInfo.Expiration NewAbsolute(long ms)
			{
				return new CacheDirectiveInfo.Expiration(ms, false);
			}

			private readonly long ms;

			private readonly bool isRelative;

			private Expiration(long ms, bool isRelative)
			{
				if (isRelative)
				{
					Preconditions.CheckArgument(ms <= MaxRelativeExpiryMs, "Expiration time is too far in the future!"
						);
				}
				this.ms = ms;
				this.isRelative = isRelative;
			}

			/// <returns>
			/// true if Expiration was specified as a relative duration, false if
			/// specified as an absolute time.
			/// </returns>
			public virtual bool IsRelative()
			{
				return isRelative;
			}

			/// <returns>
			/// The raw underlying millisecond value, either a relative duration
			/// or an absolute time as milliseconds since the Unix epoch.
			/// </returns>
			public virtual long GetMillis()
			{
				return ms;
			}

			/// <returns>
			/// Expiration time as a
			/// <see cref="System.DateTime"/>
			/// object. This converts a
			/// relative Expiration into an absolute Date based on the local
			/// clock.
			/// </returns>
			public virtual DateTime GetAbsoluteDate()
			{
				return Sharpen.Extensions.CreateDate(GetAbsoluteMillis());
			}

			/// <returns>
			/// Expiration time in milliseconds from the Unix epoch. This
			/// converts a relative Expiration into an absolute time based on the
			/// local clock.
			/// </returns>
			public virtual long GetAbsoluteMillis()
			{
				if (!isRelative)
				{
					return ms;
				}
				else
				{
					return new DateTime().GetTime() + ms;
				}
			}

			public override string ToString()
			{
				if (isRelative)
				{
					return DFSUtil.DurationToString(ms);
				}
				return DFSUtil.DateToIso8601String(Sharpen.Extensions.CreateDate(ms));
			}
		}

		private readonly long id;

		private readonly Path path;

		private readonly short replication;

		private readonly string pool;

		private readonly CacheDirectiveInfo.Expiration expiration;

		internal CacheDirectiveInfo(long id, Path path, short replication, string pool, CacheDirectiveInfo.Expiration
			 expiration)
		{
			this.id = id;
			this.path = path;
			this.replication = replication;
			this.pool = pool;
			this.expiration = expiration;
		}

		/// <returns>The ID of this directive.</returns>
		public virtual long GetId()
		{
			return id;
		}

		/// <returns>The path used in this request.</returns>
		public virtual Path GetPath()
		{
			return path;
		}

		/// <returns>The number of times the block should be cached.</returns>
		public virtual short GetReplication()
		{
			return replication;
		}

		/// <returns>The pool used in this request.</returns>
		public virtual string GetPool()
		{
			return pool;
		}

		/// <returns>When this directive expires.</returns>
		public virtual CacheDirectiveInfo.Expiration GetExpiration()
		{
			return expiration;
		}

		public override bool Equals(object o)
		{
			if (o == null)
			{
				return false;
			}
			if (GetType() != o.GetType())
			{
				return false;
			}
			CacheDirectiveInfo other = (CacheDirectiveInfo)o;
			return new EqualsBuilder().Append(GetId(), other.GetId()).Append(GetPath(), other
				.GetPath()).Append(GetReplication(), other.GetReplication()).Append(GetPool(), other
				.GetPool()).Append(GetExpiration(), other.GetExpiration()).IsEquals();
		}

		public override int GetHashCode()
		{
			return new HashCodeBuilder().Append(id).Append(path).Append(replication).Append(pool
				).Append(expiration).GetHashCode();
		}

		public override string ToString()
		{
			StringBuilder builder = new StringBuilder();
			builder.Append("{");
			string prefix = string.Empty;
			if (id != null)
			{
				builder.Append(prefix).Append("id: ").Append(id);
				prefix = ", ";
			}
			if (path != null)
			{
				builder.Append(prefix).Append("path: ").Append(path);
				prefix = ", ";
			}
			if (replication != null)
			{
				builder.Append(prefix).Append("replication: ").Append(replication);
				prefix = ", ";
			}
			if (pool != null)
			{
				builder.Append(prefix).Append("pool: ").Append(pool);
				prefix = ", ";
			}
			if (expiration != null)
			{
				builder.Append(prefix).Append("expiration: ").Append(expiration);
				prefix = ", ";
			}
			builder.Append("}");
			return builder.ToString();
		}
	}
}
