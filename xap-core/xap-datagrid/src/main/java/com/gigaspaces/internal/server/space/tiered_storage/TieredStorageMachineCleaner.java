package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.start.SystemLocations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.rmi.RemoteException;

public class TieredStorageMachineCleaner {
        private static Logger logger = LoggerFactory.getLogger(TieredStorageMachineCleaner.class);

    public static void deleteTieredStorageData(String spaceName) throws RemoteException {
        logger.info("Trying to delete db of space {}", spaceName);
        Path path = SystemLocations.singleton().work("tiered-storage/" + spaceName);
        File folder = path.toFile();
        File[] files = folder.listFiles();
        if (files == null) {
            if (logger.isDebugEnabled()){
                logger.debug("Did not find db of space {} ", spaceName);
            }
        }
        for (final File file : files) {
            if (!file.delete()) {
               logger.error("Can't remove " + file.getAbsolutePath());
            }
        }
        folder.delete();
        logger.info("Successfully deleted db of space {} in path {}", spaceName, folder.getAbsolutePath());
    }


}
