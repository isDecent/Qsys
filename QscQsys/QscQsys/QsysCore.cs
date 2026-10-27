using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Crestron.SimplSharp;             				// For Basic SIMPL# Classes
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using QscQsys.Communications.Sockets;
using QscQsys.Intermediaries;
using QscQsys.ModuleFramework.Events;
using QscQsys.ModuleFramework.Logging;

namespace QscQsys
{
    /// <summary>
    /// Q-SYS Core class that manages connection and parses responses to be distributed to components and named control classes.
    /// </summary>
    public class QsysCore : IDisposable
    {

        public const double TOLERANCE = .1d;

        #region Delegates

        public delegate void IsLoggedIn(SimplSharpString id, ushort value);

        public delegate void IsRegistered(SimplSharpString id, ushort value);

        public delegate void PrimaryIsConnectedStatus(SimplSharpString id, ushort value);

        public delegate void BackupIsConnectedStatus(SimplSharpString id, ushort value);

        public delegate void CoreStatus(
            SimplSharpString id, SimplSharpString designName, ushort isRedundant, ushort isEmulator);

        public delegate void PrimaryIsActive();

        public delegate void BackupIsActive();

        public delegate void SendingCommand(SimplSharpString id, SimplSharpString command);

        public IsLoggedIn OnIsLoggedIn { get; set; }
        public IsRegistered OnIsRegistered { get; set; }
        public PrimaryIsConnectedStatus OnPrimaryIsConnected { get; set; }
        public BackupIsConnectedStatus OnBackupIsConnected { get; set; }
        public CoreStatus OnNewCoreStatus { get; set; }
        public PrimaryIsActive OnPrimaryIsActive { get; set; }
        public BackupIsActive OnBackupIsActive { get; set; }
        public SendingCommand OnSendingCommand { get; set; }

        #endregion

        private const int HeartbeatInterval = 15000;
        private const int HeartbeatTimeout = HeartbeatInterval * 3;

        private readonly CrestronQueue<string> _commandQueue = new CrestronQueue<string>(1000);
        private readonly CTimer _commandQueueTimer;
        private readonly CTimer _heartbeatTimer;
        private readonly CTimer _waitForConnection;
        //private TCPClientDevice _primaryClient;
        //private TCPClientDevice _backupClient;
        private TcpClient _primaryClient;
        private TcpClient _backupClient;
        private Logger _logger = new Logger("QSYS");
        private StringBuilder _primaryRxData = new StringBuilder();
        private StringBuilder _backupRxData = new StringBuilder();
        private readonly object _primaryResponseLock = new object();
        private readonly object _backupResponseLock = new object();
        private readonly object _primaryParseLock = new object();
        private readonly object _backupParseLock = new object();
        private readonly object _initLock = new object();

        private readonly CCriticalSection _connectionCritical = new CCriticalSection();
        private bool _isInitialized;
        private bool _primaryIsConnected;
        private bool _backupIsConnected;
        private bool _isLoggedIn;
        private bool _disposed;
        //private ushort _debug;
        private ushort _logonAttempts;
        private ushort _maxLogonAttempts = 2;
        private bool _isRedundant;
        private bool _isEmulator;
        private bool _primaryCoreActive;
        private bool _backupCoreActive;
        private bool _externalConnection;
        private bool _changeGroupCreated;
        private string _designName;
        private string _coreId;
        private string _username;
        private string _password;

        private readonly Dictionary<string, NamedComponent> _components;
        private readonly Dictionary<string, Action<QsysStateData>> _componentUpdateCallbacks;
        private readonly Dictionary<string, NamedControl> _controls;
        private readonly Dictionary<string, Action<QsysStateData>> _controlUpdateCallbacks;

        /// <summary>
        /// Initializes a new instance of the <see cref="QsysCore"/> class.
        /// </summary>
        public QsysCore()
        {
            _components = new Dictionary<string, NamedComponent>();
            _componentUpdateCallbacks = new Dictionary<string, Action<QsysStateData>>();
            _controls = new Dictionary<string, NamedControl>();
            _controlUpdateCallbacks = new Dictionary<string, Action<QsysStateData>>();
            _heartbeatTimer = new CTimer(SendHeartbeat, Timeout.Infinite);
            _commandQueueTimer = new CTimer(CommandQueueDequeue, null, 0, 50);
            _waitForConnection = new CTimer(Initialize, Timeout.Infinite);
        }

        #region Properties

        /// <summary>
        /// Gets initialzation status.
        /// </summary>
        public bool IsInitialized
        {
            get { return _isInitialized; }
        }

        /// <summary>
        /// Gets disposed status.
        /// </summary>
        public bool IsDisposed
        {
            get { return _disposed; }
        }

        /// <summary>
        /// Gets primary core connection status.
        /// </summary>
        public bool PrimaryIsConnected
        {
            get { return _primaryIsConnected; }
        }

        /// <summary>
        /// Gets backup core connection status.
        /// </summary>
        public bool SecondaryIsConnected
        {
            get { return _backupIsConnected; }
        }

        /// <summary>
        /// Gets authentication status.
        /// </summary>
        public bool IsAuthenticated
        {
            get { return _isLoggedIn; }
        }

        /// <summary>
        /// Gets debug mode.
        /// </summary>
        public ushort IsDebugMode
        {
            get { return _logger.DebugLevel > 0 ? (ushort)1 : (ushort)0; }
        }

        /// <summary>
        /// Gets or sets the max logon attempts made when trying to authorize with the core.
        /// </summary>
        public ushort MaxLogonAttemps
        {
            get { return _maxLogonAttempts; }
            set { _maxLogonAttempts = value; }
        }

        /// <summary>
        /// Gets redundant status.
        /// </summary>
        public bool IsRedundant
        {
            get { return _isRedundant; }
        }

        /// <summary>
        /// Gets emulator status.
        /// </summary>
        public bool IsEmulator
        {
            get { return _isEmulator; }
        }

        /// <summary>
        /// Gets running design name.
        /// </summary>
        public string DesignName
        {
            get { return _designName; }
        }

        /// <summary>
        /// Gets a bool representing the primary core active status.
        /// </summary>
        public bool PrimaryCoreActive
        {
            get { return _primaryCoreActive; }
        }

        /// <summary>
        /// Gets a bool representing the backup core active status.
        /// </summary>
        public bool BackupCoreActive
        {
            get { return _backupCoreActive; }
        }

        /// <summary>
        /// Get the core ID.
        /// </summary>
        public string CoreId
        {
            get { return _coreId; }
        }

        /// <summary>
        /// Set debug mode.
        /// </summary>
        /// <param name="value">Debug level to set.</param>
        /// <remarks>
        /// Debug level 0 = off
        /// Debug level 1 = Main communications
        /// Debug level 2 = Main communications and verbose console
        /// </remarks>
        public void Debug(ushort value)
        {
            _logger.DebugLevel = (DebugLevels)value;

            if (_logger.DebugLevel == DebugLevels.Disabled) return;


            _logger.PrintLine("********Qsys Debug Mode Active********");
            _logger.PrintLine("See log for details");
            _logger.LogNotice("********Qsys Debug Mode Active********");
            if (QsysCoreManager.Is3Series)
            {
                _logger.PrintLine("********Qsys Running On 3-Series********");
                _logger.LogNotice("********Qsys Running On 3-Series********");
            }
            else
            {
                _logger.PrintLine("********Qsys Running On 4-Series Or Greater********");
                _logger.LogNotice("********Qsys Running On 4-Series Or Greater********");
            }
        }

        /// <summary>
        /// Get or set the network port. If currently connected, changing the port will reconnect with the new port number.
        /// </summary>
        public ushort Port
        {
            get { return _primaryClient == null ? ushort.MinValue : _primaryClient.Port; }
            set
            {
                if (_primaryClient != null)
                {
                    _primaryClient.Port = value;
                }

                if (_backupClient != null)
                {
                    _backupClient.Port = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the primary core network host address. If currently connected, changing the host will reconnect with the new host address.
        /// </summary>
        public string PrimaryHost
        {
            get { return _primaryClient == null ? string.Empty : _primaryClient.IpAddress; }
            set
            {
                if (_primaryClient == null) return;

                    _primaryClient.IpAddress = value;
            }
        }

        /// <summary>
        /// Gets or sets the backup core network host address. If currently connected, changing the host will reconnect with the new host address.
        /// </summary>
        public string BackupHost
        {
            get { return _backupClient == null ? string.Empty : _backupClient.IpAddress; }
            set
            {
                if (_backupClient == null) return;

                _backupClient.IpAddress = value;
            }
        }

        /// <summary>
        /// Gets or sets the username used to authenticate with the core.
        /// </summary>
        public string Username
        {
            get { return _username; }
            set { _username = value; }
        }

        /// <summary>
        /// Sets the password used to authenticate with the core.
        /// </summary>
        public string Password
        {
            set { _password = value; }
        }

    #endregion

        #region Initialization
        /// <summary>
        /// Initialzes all methods that are required to setup the class. Connection is established on port 1702.
        /// </summary>
        public void Initialize(string id, string primaryHost, string backupHost, ushort port, string username, string password, ushort useExternalConnection)
        {
            lock (_initLock)
            {
                if (_isInitialized)
                    return;

                try
                {
                    _coreId = id;
                    _logger = new Logger(string.Format("QSYS--{0}", _coreId));

                    _externalConnection = Convert.ToBoolean(useExternalConnection);

                    _username = username.Length > 0 ? username : string.Empty;

                    _password = password.Length > 0 ? password : string.Empty;

                    QsysCoreManager.AddCore(this);

                    _logger.LogNotice("QsysProcessor is initializing");

                    if (_externalConnection)
                    {
                        _primaryCoreActive = true;
                        if (OnPrimaryIsActive != null)
                            OnPrimaryIsActive();
                        return;
                    }

                    _primaryClient = new TcpClient(string.Format("QSYS--{0}--PrimaryTcpClient", _coreId), _logger);
                    _backupClient = new TcpClient(string.Format("QSYS--{0}--BackupTcpClient", _coreId), _logger);
                    //_primaryClient = new TCPClientDevice {Debug = _debug, ID = id + "-primary"};
                    //_backupClient = new TCPClientDevice {Debug = _debug, ID = id + "-backup"};

                    _primaryClient.ConnectedChange += primaryClient_ConnectedChange;
                    //_primaryClient.ConnectionStatus += primaryClient_ConnectionStatus;
                    _primaryClient.ResponseReceived += primaryClient_ResponseReceived;
                    //_primaryClient.ResponseString += primaryClient_ResponseString;
                    _backupClient.ConnectedChange += backupClient_ConnectedChange;
                    //_backupClient.ConnectionStatus += backupClient_ConnectionStatus;
                    _backupClient.ResponseReceived += backupClient_ResponseReceived;
                    //_backupClient.ResponseString += backupClient_ResponseString;

                    // The heartbeat is answered by the Core, so a silent socket means the connection is gone
                    _primaryClient.ReceiveTimeout = HeartbeatTimeout;
                    _backupClient.ReceiveTimeout = HeartbeatTimeout;
                    _heartbeatTimer.Reset(HeartbeatInterval, HeartbeatInterval);

                    _primaryClient.Connect(primaryHost, port);
                    if(backupHost != string.Empty)
                        _backupClient.Connect(backupHost, port);
                }
                catch (Exception e)
                {
                    _logger.LogException(e);
                }
            }
        }

        private void AddComponentToChangeGroup(NamedComponent component)
        {
            if (!component.Subscribe)
                return;

            var addComponent = QscQsys.AddComponentToChangeGroup.Instantiate(component.ToComponentSubscribeControls());
            Enqueue(JsonConvert.SerializeObject(addComponent));
        }

        private void AddControlToChangeGroup(NamedControl control)
        {
            AddControlsToChangeGroup(new[] {control});
        }

        private void AddControlsToChangeGroup(IEnumerable<NamedControl> controls)
        {
            var addControls = QscQsys.AddControlToChangeGroup.Instantiate(controls.Where(c => c.Subscribe).Select(c => c.Name));
            Enqueue(JsonConvert.SerializeObject(addControls));
        } 

        private void Initialize(object o)
        {
            lock (_initLock)
            {

                if(( _primaryCoreActive && !_primaryIsConnected) || (_backupCoreActive && !_backupIsConnected))
                    return;

                var components = GetNamedComponents().ToArray();
                components.ForEach(AddComponentToChangeGroup);


                if ((_primaryCoreActive && !_primaryIsConnected) || (_backupCoreActive && !_backupIsConnected))
                    return;

                var controls = GetNamedControls().ToArray();
                AddControlsToChangeGroup(controls);

                if ((_primaryCoreActive && !_primaryIsConnected) || (_backupCoreActive && !_backupIsConnected))
                    return;

                if(components.Any() || controls.Any())
                    StartAutoPoll();

                if (_externalConnection)
                    _heartbeatTimer.Reset(HeartbeatInterval, HeartbeatInterval);

                _logger.LogNotice("QsysProcessor is initialized.");

                _isInitialized = true;


                if (OnIsRegistered != null)
                    OnIsRegistered(_coreId, 1);
            }
        }

        private void ResetInitialization()
        {
            lock (_initLock)
            {
                _changeGroupCreated = false;
                _isLoggedIn = false;
                _isInitialized = false;

                // Internal connections keep the heartbeat running so the sockets can detect a dead link
                if (_externalConnection)
                    _heartbeatTimer.Stop();
                _commandQueue.Clear();

                if (OnIsRegistered != null)
                    OnIsRegistered(_coreId, 0);

                if (OnIsLoggedIn != null)
                    OnIsLoggedIn(_coreId, 0);
            }
        }

        private void StartAutoPoll()
        {
            if (((_primaryCoreActive && !_primaryIsConnected) || (_backupCoreActive && !_backupIsConnected)) || _changeGroupCreated)
                return;

            _changeGroupCreated = true;
            _commandQueue.Enqueue(JsonConvert.SerializeObject(new CreateChangeGroup()));
        }

        #endregion

        #region Named Components

        public NamedComponent LazyLoadNamedComponent(string name)
        {
            NamedComponent component;
            lock (_components)
            {
                
                if (_components.TryGetValue(name, out component))
                    return component;

                Action<QsysStateData> updateCallback;
                component = NamedComponent.Create(name, this, out updateCallback);
                _components.Add(name, component);
                _componentUpdateCallbacks.Add(name, updateCallback);
                
            }

            SubscribeComponent(component);
            AddComponentToChangeGroup(component);

            return component;
        }

        public bool TryGetNamedComponent(string name, out NamedComponent component)
        {
            lock (_components)
            {
                return _components.TryGetValue(name, out component);
            }
        }

        private bool TryGetNamedComponentUpdateCallback(string name, out Action<QsysStateData> updateCallback)
        {
            lock (_components)
            {
                return _componentUpdateCallbacks.TryGetValue(name, out updateCallback);
            }
        }

        public IEnumerable<NamedComponent> GetNamedComponents()
        {
            lock (_components)
            {
                return _components.Values.ToArray();
            }
        }

        #endregion

        #region Named Component Callbacks

        private void SubscribeComponent(NamedComponent component)
        {
            if (component == null)
                return;

            component.OnComponentControlAdded += ComponentOnComponentControlAdded;
            component.OnComponentSubscribeChanged += ComponentOnComponentSubscribeChanged;
        }

        private void UnsubscribeComponent(NamedComponent component)
        {
            if (component == null)
                return;

            component.OnComponentControlAdded -= ComponentOnComponentControlAdded;
            component.OnComponentSubscribeChanged -= ComponentOnComponentSubscribeChanged;
        }

        private void ComponentOnComponentControlAdded(object sender, ComponentControlEventArgs args)
        {
            if (IsInitialized && args.Control.Subscribe)
                AddComponentToChangeGroup(args.Control.Component);
        }

        private void ComponentOnComponentSubscribeChanged(object sender, ComponentControlSubscribeEventArgs args)
        {
            if (args.Subscribe)
                AddComponentToChangeGroup(args.Control.Component);
        }

        #endregion

        #region Named Controls

        public NamedControl LazyLoadNamedControl(string name)
        {
            return LazyLoadNamedControl(name, true);
        }

        public NamedControl LazyLoadNamedControl(string name, bool subscribe)
        {
            NamedControl control;
            lock (_controls)
            {
                if (_controls.TryGetValue(name, out control))
                {
                    // Set subscribe on existing controls (if needed)
                    if (subscribe)
                        control.SetSubscribe();
                    return control;
                }

                Action<QsysStateData> updateCallback;
                control = NamedControl.Create(name, this, subscribe, out updateCallback);
                _controls.Add(name, control);
                _controlUpdateCallbacks.Add(name, updateCallback);
            }

            SubscribeControl(control);
            if (subscribe)
                AddControlToChangeGroup(control);
            return control;
        }

        public bool TryGetNamedControl(string name, out NamedControl control)
        {
            lock (_controls)
            {
                return _controls.TryGetValue(name, out control);
            }
        }

        private bool TryGetNamedControlUpdateCallback(string name, out Action<QsysStateData> updateCallback)
        {
            lock (_controls)
            {
                return _controlUpdateCallbacks.TryGetValue(name, out updateCallback);
            }
        }

        public IEnumerable<NamedControl> GetNamedControls()
        {
            lock (_controls)
            {
                return _controls.Values.ToArray();
            }
        }

        #endregion

        #region NamedControlCallbacks

        private void SubscribeControl(NamedControl control)
        {
            if (control == null)
                return;

            control.OnSubscribeChanged += ControlOnSubscribeChanged;
        }

        private void UnsubscribeControl(NamedControl control)
        {
            if (control == null)
                return;

            control.OnSubscribeChanged -= ControlOnSubscribeChanged;
        }

        private void ControlOnSubscribeChanged(object sender, DataBoolEventArgs args)
        {
            if (!args.Data)
                return;

            var control = sender as NamedControl;
            if (control != null)
                AddControlToChangeGroup(control);
        }

        #endregion

        #region TCP Client Events
        private void primaryClient_ResponseReceived(object sender, StringEventArgs args)
        {
            lock (_primaryParseLock)
            {
                _primaryRxData.Append(args.Payload);
            }

            if (!CMonitor.TryEnter(_primaryResponseLock)) return;

            try
            {
                while (true)
                {
                    string responseData = null;

                    lock (_primaryParseLock)
                    {
                        var delimeterPos = _primaryRxData.ToString().IndexOf("\x00", StringComparison.Ordinal);
                        if (delimeterPos < 0)
                            break;

                        responseData = _primaryRxData.ToString(0, delimeterPos);
                        _primaryRxData.Remove(0, delimeterPos + 1);
                    }

                    _logger.PrintLine("Primary response found ** {0} **", responseData);

                    ParseInternalResponse(true, responseData);
                }
            }
            finally
            {
                CMonitor.Exit(_primaryResponseLock);
            }
        }

        private void backupClient_ResponseReceived(object sender, StringEventArgs args)
        {
            lock (_backupParseLock)
            {
                _backupRxData.Append(args.Payload);
            }

            if (!CMonitor.TryEnter(_backupResponseLock)) return;

            try
            {
                while (true)
                {
                    string responseData = null;

                    lock (_backupParseLock)
                    {
                        var delimeterPos = _backupRxData.ToString().IndexOf("\x00", StringComparison.Ordinal);
                        if (delimeterPos < 0)
                            break;

                        responseData = _backupRxData.ToString(0, delimeterPos);
                        _backupRxData.Remove(0, delimeterPos + 1);
                    }

                    _logger.PrintLine("Backup response found ** {0} **", responseData);

                    ParseInternalResponse(false, responseData);
                }
            }
            finally
            {
                CMonitor.Exit(_backupResponseLock);
            }
        }

        private void primaryClient_ConnectedChange(object sender, ModuleFramework.Events.BoolEventArgs args)
        {
            try
            {
                _connectionCritical.Enter();
                if (args.Payload == 1 && !_primaryIsConnected)
                {
                    _primaryIsConnected = true;

                    _logger.LogNotice("QsysProcessor primary is connected.");

                    if (OnPrimaryIsConnected != null)
                        OnPrimaryIsConnected(_coreId, 1);
                }
                else if (_primaryIsConnected && args.Payload != 1)
                {
                    _primaryIsConnected = false;

                    _logger.LogError("QsysProcessor primary disconnected!");

                    if (_primaryCoreActive) ResetInitialization();

                    if (OnPrimaryIsConnected != null)
                        OnPrimaryIsConnected(_coreId, 0);
                }
            }
            catch (Exception e)
            {
                _logger.LogException(e);
            }
            finally
            {
                _connectionCritical.Leave();
            }
        }

        private void backupClient_ConnectedChange(object sender, ModuleFramework.Events.BoolEventArgs args)
        {
            try
            {
                _connectionCritical.Enter();
                if (args.Payload == 1 && !_backupIsConnected)
                {
                    _backupIsConnected = true;

                    _logger.LogNotice("QsysProcessor backup is connected.");

                    if (OnBackupIsConnected != null)
                        OnBackupIsConnected(_coreId, 1);
                }
                else if (_backupIsConnected && args.Payload != 1)
                {
                    _backupIsConnected = false;

                    _logger.LogError("QsysProcessor backup disconnected!");

                    if (_backupCoreActive) ResetInitialization();

                    if (OnBackupIsConnected != null)
                        OnBackupIsConnected(_coreId, 0);
                }
            }
            catch (Exception e)
            {
                _logger.LogException(e);
            }
            finally
            {
                _connectionCritical.Leave();
            }
        }

        private void SendHeartbeat(object o)
        {
            _commandQueue.Enqueue(JsonConvert.SerializeObject(new Heartbeat()));
        }
        #endregion

        #region Parsing
        private void ParseInternalResponse(bool primaryCore, string returnString)
        {
            try
            {
                if (returnString.Length <= 0) return;

                if (returnString.Contains("Changes") && !returnString.Contains("\"Changes\":[]"))
                {
                    var response = JObject.Parse(returnString);
                    var changes = response["params"]["Changes"].Children().ToList();

                    foreach (var change in changes)
                    {
                        var changeResult = JsonConvert.DeserializeObject<ChangeResult>(change.ToString(),
                            new JsonSerializerSettings
                            { MissingMemberHandling = MissingMemberHandling.Ignore });

                        if (changeResult.Component != null)
                        {
                            var choices = changeResult.Choices != null ? changeResult.Choices.ToList() : new List<string>();

                            Action<QsysStateData> updateCallback;
                            if (!TryGetNamedComponentUpdateCallback(changeResult.Component, out updateCallback))
                                continue;

                            updateCallback(new QsysStateData("change", changeResult.Name,
                                changeResult.Value,
                                changeResult.Position,
                                changeResult.String, choices));
                        }
                        else if (changeResult.Name != null)
                        {
                            List<string> choices = changeResult.Choices != null ? changeResult.Choices.ToList() : new List<string>();

                            Action<QsysStateData> controlUpdateCallback;
                            if (!TryGetNamedControlUpdateCallback(changeResult.Name, out controlUpdateCallback))
                                continue;

                            controlUpdateCallback(new QsysStateData("change", changeResult.Name,
                                changeResult.Value,
                                changeResult.Position,
                                changeResult.String, choices));
                        }
                    }
                }
                else if (returnString.Contains("EngineStatus"))
                {
                    var response = JObject.Parse(returnString);

                    if (_externalConnection)
                    {
                        _isLoggedIn = false;
                    }
                    if (response["params"] != null)
                    {
                        var engineStatus = response["params"];

                        if (engineStatus["State"] != null)
                        {
                            if (primaryCore && engineStatus["State"].ToObject<string>() == "Active")
                            {
                                if (_backupCoreActive)
                                {

                                    _backupCoreActive = false;
                                    ResetInitialization();
                                }
                                _primaryCoreActive = true;
                                if (OnPrimaryIsActive != null)
                                    OnPrimaryIsActive();
                            }
                            else if (!primaryCore && engineStatus["State"].ToObject<string>() == "Active")
                            {
                                if (_primaryCoreActive)
                                {
                                    _primaryCoreActive = false;
                                    ResetInitialization();
                                }
                                _backupCoreActive = true;
                                if (OnBackupIsActive != null)
                                    OnBackupIsActive();
                            }
                        }

                        if ((primaryCore && _primaryCoreActive) || (!primaryCore && _backupCoreActive))
                        {

                            if (engineStatus["DesignName"] != null)
                            {
                                _designName = engineStatus["DesignName"].ToString();
                            }

                            if (engineStatus["IsRedundant"] != null)
                            {
                                _isRedundant = Convert.ToBoolean(engineStatus["IsRedundant"].ToString());
                            }

                            if (engineStatus["IsEmulator"] != null)
                            {
                                _isEmulator = Convert.ToBoolean(engineStatus["IsEmulator"].ToString());
                            }

                            if (OnNewCoreStatus != null)
                                OnNewCoreStatus(_coreId, _designName, Convert.ToUInt16(_isRedundant), Convert.ToUInt16(_isEmulator));
                        }
                    }

                    if (_isLoggedIn) return;
                    _logger.LogNotice("QsysProcessor server ready, starting to send intialization strings.");

                    if (_password.Length > 0 && _username.Length > 0)
                    {
                        _logonAttempts = 1;
                        _commandQueue.Enqueue(JsonConvert.SerializeObject(new Logon { Params = new LogonParams { User = _username, Password = _password } }));
                    }
                    else if((primaryCore && _primaryCoreActive) || (!primaryCore && _backupCoreActive))
                    {
                        _isLoggedIn = true;

                        if (OnIsLoggedIn != null)
                        {
                            OnIsLoggedIn(_coreId, 1);
                        }

                        _waitForConnection.Reset(5000);
                    }
                }
                else if (returnString.Contains("error"))
                {
                    var response = JObject.Parse(returnString);

                    if (_logonAttempts < _maxLogonAttempts)
                    {
                        var error = response["error"];

                        if (error["code"] != null)
                        {
                            if (error["code"].ToString().Replace("\'", string.Empty) == "10")
                            {
                                _logonAttempts++;
                                _commandQueue.Enqueue(JsonConvert.SerializeObject(new Logon { Params = new LogonParams { User = _username, Password = _password } }));
                            }
                        }
                    }
                    else
                    {
                        _logger.LogError("Error in QsysProcessor max logon attempts reached");
                    }
                }
                else if (returnString.Contains("\"id\":") && returnString.Contains("\"result\":true"))
                {
                    var response = JObject.Parse(returnString);

                    if (response["id"] != null)
                    {
                        var responseData = JsonConvert.DeserializeObject<CustomResponseId>(response["id"].ToObject<string>());

                        if (responseData.Method == "Logon")
                        {
                            _isLoggedIn = true;

                            if (OnIsLoggedIn != null)
                            {
                                OnIsLoggedIn(_coreId, 1);
                            }

                            _waitForConnection.Reset(5000);
                        }

                        if (responseData.Caller != string.Empty)
                        {
                            Action<QsysStateData> updateCallback;
                            if (!TryGetNamedComponentUpdateCallback(responseData.Caller, out updateCallback))
                                return;

                            updateCallback(new QsysStateData(responseData.ValueType,
                                responseData.Method,
                                responseData.Value,
                                responseData.Position,
                                responseData.StringValue,
                                null));
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogException(e);
            }
        }

        /// <summary>
        /// Enqueue response from SIMPL to be parsed
        /// </summary>
        /// <param name="response">Response from SIMPL to be parsed</param>
        public void NewExternalResponse(string response)
        {
            ParseInternalResponse(true, response);
            //ProcessResponse(true, response);
        }
        #endregion

        #region Command Queue
        internal void Enqueue(string data)
        {
            if (data.Length > 0)
                _commandQueue.Enqueue(data);
        }

        private void CommandQueueDequeue(object o)
        {
            var externalSendCallback = OnSendingCommand;

            if (!_externalConnection)
            {
                if (_primaryCoreActive && _primaryClient == null)
                    return;

                if (_backupCoreActive && _backupClient == null)
                    return;
            }
            if (_externalConnection && externalSendCallback == null)
                return;
            if (_commandQueue.IsEmpty)
                return;

            try
            {
                var data = _commandQueue.TryToDequeue();

                if (data == null)
                    return;

                _logger.PrintLine("Command sent -->{0}<--", data);

                if (!_externalConnection)
                {
                    if (data.Contains("NoOp"))
                    {
                        _primaryClient.SendCommand(data + "\x00");
                        _backupClient.SendCommand(data + "\x00");
                    }
                    else if (_primaryCoreActive)
                    {
                        _primaryClient.SendCommand(data + "\x00");
                    }
                    else if (_backupCoreActive)
                    {
                        _backupClient.SendCommand(data + "\x00");
                    }
                }
                else
                {
                    data = data + "\x00";
                    var xs = data.Chunk(200);

                    foreach (var x in xs)
                    {
                        _logger.PrintLine("Command chunk sent externally length={0} -->{1}<--", x.Length, x);
                        externalSendCallback(_coreId, x);
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogException(e);

                _commandQueue.Clear();
            }
        }
        #endregion

        /// <summary>
        /// Clean up of unmanaged resources
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
        }

        private void Dispose(bool disposing)
        {
            if (_disposed) return;

            _disposed = true;
            if (disposing)
            {
                _changeGroupCreated = false;
                _isLoggedIn = false;
                _isInitialized = false;

                _waitForConnection.Dispose();
                _heartbeatTimer.Dispose();
                _commandQueueTimer.Dispose();
                _commandQueue.Dispose();

                _primaryClient.Dispose();
                _backupClient.Dispose();              

                _primaryRxData = null;
                _backupRxData = null;
            }
        }
    }
}