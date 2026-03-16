[Setup]
AppName=TorqMind Agent
AppVersion=1.0
DefaultDirName={pf}\TorqMindAgent
DefaultGroupName=TorqMind
OutputBaseFilename=TorqMindAgentInstaller
Compression=lzma
SolidCompression=yes
PrivilegesRequired=admin
ArchitecturesInstallIn64BitMode=x64compatible
WizardStyle=modern

[Files]
Source: "..\..\release\torqmind-agent.exe"; DestDir: "{app}"
Source: "..\..\release\torqmind-agent-service.exe"; DestDir: "{app}"
Source: "..\..\release\torqmind-agent-service.xml.template"; DestDir: "{app}"
Source: "..\..\release\update-config.bat"; DestDir: "{app}"

[Dirs]
Name: "{app}\logs"
Name: "{app}\state"
Name: "{app}\spool"

[Code]
var
  ConfigPage: TInputQueryWizardPage;

function CmdQuote(const Value: string): string;
begin
  Result := '"' + StringChangeEx(Value, '"', '\"', True) + '"';
end;

function ServiceXmlContent(): string;
begin
  Result :=
    '<service>' + #13#10 +
    '  <id>TorqMindAgent</id>' + #13#10 +
    '  <name>TorqMind Agent</name>' + #13#10 +
    '  <description>TorqMind ETL Agent</description>' + #13#10 +
    '  <executable>%BASE%\torqmind-agent.exe</executable>' + #13#10 +
    '  <arguments>run --loop --interval ' + Trim(ConfigPage.Values[7]) + ' --config "%BASE%\config.enc"</arguments>' + #13#10 +
    '  <workingdirectory>%BASE%</workingdirectory>' + #13#10 +
    '  <startmode>Automatic</startmode>' + #13#10 +
    '  <delayedAutoStart>true</delayedAutoStart>' + #13#10 +
    '  <stoptimeout>15 sec</stoptimeout>' + #13#10 +
    '  <log mode="roll-by-size">' + #13#10 +
    '    <sizeThreshold>10240</sizeThreshold>' + #13#10 +
    '    <keepFiles>8</keepFiles>' + #13#10 +
    '  </log>' + #13#10 +
    '  <onfailure action="restart" delay="10 sec"/>' + #13#10 +
    '  <onfailure action="restart" delay="30 sec"/>' + #13#10 +
    '  <onfailure action="restart" delay="60 sec"/>' + #13#10 +
    '</service>' + #13#10;
end;

procedure InitializeWizard();
begin
  ConfigPage := CreateInputQueryPage(
    wpSelectDir,
    'Configuração inicial',
    'Informe os dados públicos e sensíveis do Agent',
    'Toda a configuração será gravada em config.enc com DPAPI.'
  );

  ConfigPage.Add('API base_url:', False);
  ConfigPage.Add('API ingest_key:', True);
  ConfigPage.Add('SQL host:', False);
  ConfigPage.Add('SQL port:', False);
  ConfigPage.Add('SQL database:', False);
  ConfigPage.Add('SQL username:', False);
  ConfigPage.Add('SQL password:', True);
  ConfigPage.Add('Intervalo de sync (segundos):', False);

  ConfigPage.Values[0] := 'https://torqmind.com/api';
  ConfigPage.Values[3] := '1433';
  ConfigPage.Values[7] := '60';
end;

function NextButtonClick(CurPageID: Integer): Boolean;
begin
  Result := True;
  if CurPageID = ConfigPage.ID then
  begin
    if Trim(ConfigPage.Values[0]) = '' then
      RaiseException('API base_url é obrigatório.');
    if Trim(ConfigPage.Values[1]) = '' then
      RaiseException('API ingest_key é obrigatório.');
    if Trim(ConfigPage.Values[2]) = '' then
      RaiseException('SQL host é obrigatório.');
    if Trim(ConfigPage.Values[3]) = '' then
      RaiseException('SQL port é obrigatório.');
    if Trim(ConfigPage.Values[4]) = '' then
      RaiseException('SQL database é obrigatório.');
    if Trim(ConfigPage.Values[5]) = '' then
      RaiseException('SQL username é obrigatório.');
    if Trim(ConfigPage.Values[6]) = '' then
      RaiseException('SQL password é obrigatório.');
    if Trim(ConfigPage.Values[7]) = '' then
      RaiseException('Intervalo de sync é obrigatório.');
  end;
end;

procedure RunOrFail(const FileName: string; const Params: string; const FailureContext: string);
var
  ResultCode: Integer;
begin
  if not Exec(FileName, Params, '', SW_HIDE, ewWaitUntilTerminated, ResultCode) then
    RaiseException('Falha ao executar ' + FailureContext + '.');
  if ResultCode <> 0 then
    RaiseException(FailureContext + ' retornou erro.');
end;

procedure CurStepChanged(CurStep: TSetupStep);
var
  AppDir: string;
  ConfigPath: string;
  ServiceXmlPath: string;
  AgentExe: string;
  ServiceExe: string;
begin
  if CurStep <> ssPostInstall then
    Exit;

  AppDir := ExpandConstant('{app}');
  ConfigPath := AppDir + '\config.enc';
  ServiceXmlPath := AppDir + '\torqmind-agent-service.xml';
  AgentExe := AppDir + '\torqmind-agent.exe';
  ServiceExe := AppDir + '\torqmind-agent-service.exe';

  SaveStringToFile(ServiceXmlPath, ServiceXmlContent(), False);

  RunOrFail(
    AgentExe,
    'config init --config ' + CmdQuote(ConfigPath) +
    ' --api-base-url ' + CmdQuote(ConfigPage.Values[0]) +
    ' --ingest-key ' + CmdQuote(ConfigPage.Values[1]) +
    ' --sql-host ' + CmdQuote(ConfigPage.Values[2]) +
    ' --sql-port ' + CmdQuote(ConfigPage.Values[3]) +
    ' --sql-database ' + CmdQuote(ConfigPage.Values[4]) +
    ' --sql-username ' + CmdQuote(ConfigPage.Values[5]) +
    ' --sql-password ' + CmdQuote(ConfigPage.Values[6]) +
    ' --interval-seconds ' + CmdQuote(ConfigPage.Values[7]),
    'a configuração inicial do Agent'
  );

  RunOrFail(
    ExpandConstant('{cmd}'),
    '/C icacls "' + AppDir + '" /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "Administrators:(OI)(CI)F" /T /C',
    'o endurecimento de permissões locais'
  );

  RunOrFail(ServiceExe, 'install', 'a instalação do serviço Windows');
  RunOrFail(ExpandConstant('{sys}\sc.exe'), 'config TorqMindAgent start= delayed-auto', 'a configuração de delayed auto-start');
  RunOrFail(
    ExpandConstant('{sys}\sc.exe'),
    'failure TorqMindAgent reset= 86400 actions= restart/10000/restart/30000/restart/60000',
    'a política de restart automático do serviço'
  );
  RunOrFail(ExpandConstant('{sys}\sc.exe'), 'failureflag TorqMindAgent 1', 'a ativação do restart on failure');
  RunOrFail(ServiceExe, 'start', 'a inicialização do serviço Windows');
end;
