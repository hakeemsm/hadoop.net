using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.Event
{
	/// <summary>
	/// Event indicating that the
	/// <see cref="Org.Apache.Hadoop.Yarn.Server.Nodemanager.Containermanager.Localizer.ResourceLocalizationService
	/// 	"/>
	/// should fetch this resource.
	/// </summary>
	public class LocalizerResourceRequestEvent : LocalizerEvent
	{
		private readonly LocalizerContext context;

		private readonly LocalizedResource resource;

		private readonly LocalResourceVisibility vis;

		private readonly string pattern;

		public LocalizerResourceRequestEvent(LocalizedResource resource, LocalResourceVisibility
			 vis, LocalizerContext context, string pattern)
			: base(LocalizerEventType.RequestResourceLocalization, ConverterUtils.ToString(context
				.GetContainerId()))
		{
			this.vis = vis;
			this.context = context;
			this.resource = resource;
			this.pattern = pattern;
		}

		public virtual LocalizedResource GetResource()
		{
			return resource;
		}

		public virtual LocalizerContext GetContext()
		{
			return context;
		}

		public virtual LocalResourceVisibility GetVisibility()
		{
			return vis;
		}

		public virtual string GetPattern()
		{
			return pattern;
		}
	}
}
