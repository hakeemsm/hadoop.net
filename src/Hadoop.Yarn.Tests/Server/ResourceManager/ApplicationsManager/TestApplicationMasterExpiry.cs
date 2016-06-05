using Sharpen;

namespace Org.Apache.Hadoop.Yarn.Server.Resourcemanager.Applicationsmanager
{
	/// <summary>A test case that tests the expiry of the application master.</summary>
	/// <remarks>
	/// A test case that tests the expiry of the application master.
	/// More tests can be added to this.
	/// </remarks>
	public class TestApplicationMasterExpiry
	{
		//  private static final Log LOG = LogFactory.getLog(TestApplicationMasterExpiry.class);
		//  private static RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
		//
		//  private final RMContext context = new RMContextImpl(new MemStore());
		//  private AMLivelinessMonitor amLivelinessMonitor;
		//
		//  @Before
		//  public void setUp() {
		//    new DummyApplicationTracker();
		//    new DummySN();
		//    new DummyLauncher();
		//    new ApplicationEventTypeListener();
		//    Configuration conf = new Configuration();
		//    context.getDispatcher().register(ApplicationEventType.class,
		//        new ResourceManager.ApplicationEventDispatcher(context));
		//    context.getDispatcher().init(conf);
		//    context.getDispatcher().start();
		//    conf.setLong(YarnConfiguration.AM_EXPIRY_INTERVAL, 1000L);
		//    amLivelinessMonitor = new AMLivelinessMonitor(this.context
		//        .getDispatcher().getEventHandler());
		//    amLivelinessMonitor.init(conf);
		//    amLivelinessMonitor.start();
		//  }
		//
		//  private class DummyApplicationTracker implements EventHandler<ASMEvent<ApplicationTrackerEventType>> {
		//    DummyApplicationTracker() {
		//      context.getDispatcher().register(ApplicationTrackerEventType.class, this);
		//    }
		//    @Override
		//    public void handle(ASMEvent<ApplicationTrackerEventType> event) {
		//    }
		//  }
		//
		//  private AtomicInteger expiry = new AtomicInteger();
		//  private boolean expired = false;
		//
		//  private class ApplicationEventTypeListener implements
		//      EventHandler<ApplicationEvent> {
		//    ApplicationEventTypeListener() {
		//      context.getDispatcher().register(ApplicationEventType.class, this);
		//    }
		//    @Override
		//    public void handle(ApplicationEvent event) {
		//      switch(event.getType()) {
		//      case EXPIRE:
		//        expired = true;
		//        LOG.info("Received expiry from application " + event.getApplicationId());
		//        synchronized(expiry) {
		//          expiry.addAndGet(1);
		//        }
		//      }
		//    }
		//  }
		//
		//  private class DummySN implements EventHandler<ASMEvent<SNEventType>> {
		//    DummySN() {
		//      context.getDispatcher().register(SNEventType.class, this);
		//    }
		//    @Override
		//    public void handle(ASMEvent<SNEventType> event) {
		//    }
		//  }
		//
		//  private class DummyLauncher implements EventHandler<ASMEvent<AMLauncherEventType>> {
		//    DummyLauncher() {
		//      context.getDispatcher().register(AMLauncherEventType.class, this);
		//    }
		//    @Override
		//    public void handle(ASMEvent<AMLauncherEventType> event) {
		//    }
		//  }
		//
		//  private void waitForState(AppAttempt application, ApplicationState
		//      finalState) throws Exception {
		//    int count = 0;
		//    while(application.getState() != finalState && count < 10) {
		//      Thread.sleep(500);
		//      count++;
		//    }
		//    Assert.assertEquals(finalState, application.getState());
		//  }
		//
		//  @Test
		//  public void testAMExpiry() throws Exception {
		//    ApplicationSubmissionContext submissionContext = recordFactory
		//        .newRecordInstance(ApplicationSubmissionContext.class);
		//    submissionContext.setApplicationId(recordFactory
		//        .newRecordInstance(ApplicationId.class));
		//    submissionContext.getApplicationId().setClusterTimestamp(
		//        System.currentTimeMillis());
		//    submissionContext.getApplicationId().setId(1);
		//
		//    ApplicationStore appStore = context.getApplicationsStore()
		//    .createApplicationStore(submissionContext.getApplicationId(),
		//        submissionContext);
		//    AppAttempt application = new AppAttemptImpl(context,
		//        new Configuration(), "dummy", submissionContext, "dummytoken", appStore,
		//        amLivelinessMonitor);
		//    context.getApplications()
		//        .put(application.getApplicationID(), application);
		//
		//    this.context.getDispatcher().getSyncHandler().handle(
		//        new ApplicationEvent(ApplicationEventType.ALLOCATE, submissionContext
		//            .getApplicationId()));
		//
		//    waitForState(application, ApplicationState.ALLOCATING);
		//
		//    this.context.getDispatcher().getEventHandler().handle(
		//        new AMAllocatedEvent(application.getApplicationID(),
		//            application.getMasterContainer()));
		//
		//    waitForState(application, ApplicationState.LAUNCHING);
		//
		//    this.context.getDispatcher().getEventHandler().handle(
		//        new ApplicationEvent(ApplicationEventType.LAUNCHED,
		//            application.getApplicationID()));
		//    synchronized(expiry) {
		//      while (expiry.get() == 0) {
		//        expiry.wait(1000);
		//      }
		//    }
		//    Assert.assertTrue(expired);
		//  }
	}
}
