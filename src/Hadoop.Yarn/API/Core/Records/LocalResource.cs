using Org.Apache.Hadoop.Classification;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Api.Records
{
	/// <summary>
	/// <p><code>LocalResource</code> represents a local resource required to
	/// run a container.</p>
	/// <p>The <code>NodeManager</code> is responsible for localizing the resource
	/// prior to launching the container.</p>
	/// <p>Applications can specify
	/// <see cref="LocalResourceType"/>
	/// and
	/// <see cref="LocalResourceVisibility"/>
	/// .</p>
	/// </summary>
	/// <seealso cref="LocalResourceType"/>
	/// <seealso cref="LocalResourceVisibility"/>
	/// <seealso cref="ContainerLaunchContext"/>
	/// <seealso cref="ApplicationSubmissionContext"/>
	/// <seealso cref="Org.Apache.Hadoop.Yarn.Api.ContainerManagementProtocol.StartContainers(Org.Apache.Hadoop.Yarn.Api.Protocolrecords.StartContainersRequest)
	/// 	"/>
	public abstract class LocalResource
	{
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static LocalResource NewInstance(URL url, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp, string pattern)
		{
			return NewInstance(url, type, visibility, size, timestamp, pattern, false);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static LocalResource NewInstance(URL url, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp, string pattern, bool shouldBeUploadedToSharedCache
			)
		{
			LocalResource resource = Org.Apache.Hadoop.Yarn.Util.Records.NewRecord<LocalResource
				>();
			resource.SetResource(url);
			resource.SetType(type);
			resource.SetVisibility(visibility);
			resource.SetSize(size);
			resource.SetTimestamp(timestamp);
			resource.SetPattern(pattern);
			resource.SetShouldBeUploadedToSharedCache(shouldBeUploadedToSharedCache);
			return resource;
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public static LocalResource NewInstance(URL url, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp)
		{
			return NewInstance(url, type, visibility, size, timestamp, null);
		}

		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public static LocalResource NewInstance(URL url, LocalResourceType type, LocalResourceVisibility
			 visibility, long size, long timestamp, bool shouldBeUploadedToSharedCache)
		{
			return NewInstance(url, type, visibility, size, timestamp, null, shouldBeUploadedToSharedCache
				);
		}

		/// <summary>Get the <em>location</em> of the resource to be localized.</summary>
		/// <returns><em>location</em> of the resource to be localized</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract URL GetResource();

		/// <summary>Set <em>location</em> of the resource to be localized.</summary>
		/// <param name="resource"><em>location</em> of the resource to be localized</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetResource(URL resource);

		/// <summary>Get the <em>size</em> of the resource to be localized.</summary>
		/// <returns><em>size</em> of the resource to be localized</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetSize();

		/// <summary>Set the <em>size</em> of the resource to be localized.</summary>
		/// <param name="size"><em>size</em> of the resource to be localized</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetSize(long size);

		/// <summary>
		/// Get the original <em>timestamp</em> of the resource to be localized, used
		/// for verification.
		/// </summary>
		/// <returns><em>timestamp</em> of the resource to be localized</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract long GetTimestamp();

		/// <summary>
		/// Set the <em>timestamp</em> of the resource to be localized, used
		/// for verification.
		/// </summary>
		/// <param name="timestamp"><em>timestamp</em> of the resource to be localized</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetTimestamp(long timestamp);

		/// <summary>Get the <code>LocalResourceType</code> of the resource to be localized.</summary>
		/// <returns><code>LocalResourceType</code> of the resource to be localized</returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract LocalResourceType GetType();

		/// <summary>Set the <code>LocalResourceType</code> of the resource to be localized.</summary>
		/// <param name="type"><code>LocalResourceType</code> of the resource to be localized
		/// 	</param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetType(LocalResourceType type);

		/// <summary>
		/// Get the <code>LocalResourceVisibility</code> of the resource to be
		/// localized.
		/// </summary>
		/// <returns>
		/// <code>LocalResourceVisibility</code> of the resource to be
		/// localized
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract LocalResourceVisibility GetVisibility();

		/// <summary>
		/// Set the <code>LocalResourceVisibility</code> of the resource to be
		/// localized.
		/// </summary>
		/// <param name="visibility">
		/// <code>LocalResourceVisibility</code> of the resource to be
		/// localized
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetVisibility(LocalResourceVisibility visibility);

		/// <summary>
		/// Get the <em>pattern</em> that should be used to extract entries from the
		/// archive (only used when type is <code>PATTERN</code>).
		/// </summary>
		/// <returns>
		/// <em>pattern</em> that should be used to extract entries from the
		/// archive.
		/// </returns>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract string GetPattern();

		/// <summary>
		/// Set the <em>pattern</em> that should be used to extract entries from the
		/// archive (only used when type is <code>PATTERN</code>).
		/// </summary>
		/// <param name="pattern">
		/// <em>pattern</em> that should be used to extract entries
		/// from the archive.
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Stable]
		public abstract void SetPattern(string pattern);

		/// <summary>
		/// NM uses it to decide whether if it is necessary to upload the resource to
		/// the shared cache
		/// </summary>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract bool GetShouldBeUploadedToSharedCache();

		/// <summary>Inform NM whether upload to SCM is needed.</summary>
		/// <param name="shouldBeUploadedToSharedCache">
		/// <em>shouldBeUploadedToSharedCache</em>
		/// of this request
		/// </param>
		[InterfaceAudience.Public]
		[InterfaceStability.Unstable]
		public abstract void SetShouldBeUploadedToSharedCache(bool shouldBeUploadedToSharedCache
			);
	}
}
