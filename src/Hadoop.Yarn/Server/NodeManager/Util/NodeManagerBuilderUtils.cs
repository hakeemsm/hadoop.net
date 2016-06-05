using Org.Apache.Hadoop.FS;
using Org.Apache.Hadoop.Yarn.Api.Records;
using Org.Apache.Hadoop.Yarn.Server.Nodemanager.Api;
using Org.Apache.Hadoop.Yarn.Util;
using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Nodemanager.Util
{
	public class NodeManagerBuilderUtils
	{
		public static ResourceLocalizationSpec NewResourceLocalizationSpec(LocalResource 
			rsrc, Path path)
		{
			URL local = ConverterUtils.GetYarnUrlFromPath(path);
			ResourceLocalizationSpec resourceLocalizationSpec = Org.Apache.Hadoop.Yarn.Util.Records
				.NewRecord<ResourceLocalizationSpec>();
			resourceLocalizationSpec.SetDestinationDirectory(local);
			resourceLocalizationSpec.SetResource(rsrc);
			return resourceLocalizationSpec;
		}
	}
}
