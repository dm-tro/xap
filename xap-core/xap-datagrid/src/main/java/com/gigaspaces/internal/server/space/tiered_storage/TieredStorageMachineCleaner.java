package com.gigaspaces.internal.server.space.tiered_storage;

import com.gigaspaces.start.SystemLocations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;

public class TieredStorageMachineCleaner {
        private static Logger logger = LoggerFactory.getLogger(TieredStorageMachineCleaner.class);


    public static void deleteTieredStorageData(String spaceName) {
        if (logger.isDebugEnabled()){
            logger.debug("Trying to delete db of space {}", spaceName);
        }
        Path path = SystemLocations.singleton().work("tiered-storage/" + spaceName);
        File folder = path.toFile();
        File[] files = folder.listFiles();
        if (files == null) {
            if (logger.isDebugEnabled()){
                logger.debug("Did not find db of space {} ", spaceName);
            }
        } else {
            for (final File file : files) {
                if (!file.delete()) {
                    logger.warn("Can't remove " + file.getAbsolutePath());
                }
            }
            folder.delete();
            logger.info("Successfully deleted db of space {} in path {}", spaceName, folder.getAbsolutePath());
        }
    }
}
