# nc.ps1
# http://inaz2.hatenablog.com/entry/2015/04/16/025953
Param(
    [string]$addr,
    [int]$port,
    [alias("l")][int]$lport,
    [alias("v")][switch]$verbose
)

$ErrorActionPreference = "Stop"
if ($verbose) {
    $VerbosePreference = "continue"
}

function interact($client) {
    $stream = $client.GetStream()
    $buffer = New-Object System.Byte[] $client.ReceiveBufferSize
    $enc = New-Object System.Text.AsciiEncoding

    try {
        $ar = $stream.BeginRead($buffer, 0, $buffer.length, $NULL, $NULL)
        while ($TRUE) {
            if ($ar.IsCompleted) {
                $bytes = $stream.EndRead($ar)
                if ($bytes -eq 0) {
                    break
                }
                Write-Host -n $enc.GetString($buffer, 0, $bytes)
                $ar = $stream.BeginRead($buffer, 0, $buffer.length, $NULL, $NULL)
            }
            if ($Host.UI.RawUI.KeyAvailable) {
                $data = $enc.GetBytes((Read-Host) + "`n")
                $stream.Write($data, 0, $data.length)
            }
        }
    } catch [System.IO.IOException] {
        # ignore exception at $stream.BeginRead()
    } finally {
        $stream.Close()
    }
}

if ($lport) {
    $endpoint = New-Object System.Net.IPEndPoint ([System.Net.IPAddress]::Any, $lport)
    $listener = New-Object System.Net.Sockets.TcpListener $endpoint
    $listener.Start()
    Write-Verbose "Listening on [0.0.0.0] (family 0, port $($lport))"

    $handle = $listener.BeginAcceptTcpClient($null, $null)
    while (!$handle.IsCompleted) {
        Start-Sleep -m 100
    }
    $client = $listener.EndAcceptTcpClient($handle)
    $remote = $client.Client.RemoteEndPoint
    Write-Verbose "Connection from [$($remote.Address)] port $($lport) [tcp/*] accepted (family 2, sport $($remote.Port))"

    interact $client

    $client.Close()
    $listener.Stop()
} elseif ($addr -and $port) {
    $client = New-Object System.Net.Sockets.TcpClient ($addr, $port)
    Write-Verbose "Connection to $($addr) $($port) port [tcp/*] succeeded!"

    interact $client

    $client.Close()
}

