package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.start.SystemLocations;

import java.io.File;
import java.nio.file.Path;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface InternalMachineCleanerProvider extends Remote {

     void deleteTieredStorageData(String spaceName) throws RemoteException;
}
